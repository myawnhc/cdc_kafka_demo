package com.hazelcast;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hazelcast.Main.Classifier;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.jet.cdc.ChangeRecord;
import com.hazelcast.enterprise.jet.cdc.Operation;
import com.hazelcast.enterprise.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.spi.properties.ClusterProperty;

@SuppressWarnings("SqlResolve")
public class CDC {
    private static final int UPPER_LIMIT = 100;
    private static final String TEST_KAFKA_VERSION = System.getProperty("test.kafka.version", "7.7.1");
    public static final DockerImageName DEBEZIUM_MYSQL_IMAGE = DockerImageName
            .parse("debezium/example-mysql:2.7.1.Final")
            .asCompatibleSubstituteFor("mysql");

    record ProcessedPayment(Classifier classifier, Payment payment) {
        @Override
        public String toString() {
            return String.format("%-10s :: %s", classifier, payment);
        }
    }

    public static void main(String[] args) throws Exception {
        // mvn clean install exec:java -Dexec.mainClass=com.hazelcast.CDC
        KafkaContainer kafkaContainer = new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka").withTag(TEST_KAFKA_VERSION));
        kafkaContainer.start();
        // create mysql container
        try (var mysqlContainer = new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw")
                .withExposedPorts(3306)
                .withNetworkAliases("inputDatabase")) {
            mysqlContainer.start();

            // create the tables and add master data
            createTablesAndMasterData(mysqlContainer);

            // run hazeclast instance
            HazelcastInstance hz = getHz(mysqlContainer);

            // Properties for Kafka sink
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
            props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
            props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());

            // create the pipeline
            Pipeline pipeline = getHashjoinPipeline(mysqlContainer, kafkaContainer);

            // deploy the pipeline
            hz.getJet().newJob(pipeline);

            Thread.sleep(Duration.ofSeconds(3));

            // start inserting payments
            insertPayment(mysqlContainer, Payment.generatePayment(Classifier.SUSPICIOUS));
            insertPayment(mysqlContainer, Payment.generatePayment(Classifier.LEGIT));

            RandomUtils randomUtils = RandomUtils.insecure();
            // noinspection InfiniteLoopStatement
            while (true) {
                Classifier classifier = randomUtils.randomBoolean() ? Classifier.LEGIT : Classifier.SUSPICIOUS;
                insertPayment(mysqlContainer, Payment.generatePayment(classifier));
                Thread.sleep(Duration.ofSeconds(2));
            }
        } finally {
            kafkaContainer.stop();
        }
    }

    private static Pipeline getHashjoinPipeline(MySQLContainer<?> mySqlContainer, KafkaContainer kafkaContainer) {
        String jdbcUrl = mySqlContainer.getJdbcUrl();
        String username = mySqlContainer.getUsername();
        String password = mySqlContainer.getPassword();
        ServiceFactory<?, Connection> jdbcServiceFactory = ServiceFactories.sharedService(ctx -> {
            return DriverManager.getConnection(jdbcUrl, username, password);
        }).toNonCooperative();

        Pipeline pipeline = Pipeline.create();
        var source = MySqlCdcSources.mysql("payments")
                .setDatabaseAddress(mySqlContainer.getHost(), mySqlContainer.getMappedPort(MySQLContainer.MYSQL_PORT))
                .setDatabaseCredentials("debezium", "dbz")
                .setTableIncludeList("inventory.payments")
                .changeRecord()
                .build();
        // get streaming data from CDC
        StreamStage<ChangeRecord> streamingCDCRows = pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != Operation.UNSPECIFIED)
                .setName("all but UNSPECIFIED");
        // Lets groupBy key fields and coalesce the records every 10 seconds
        StreamStage<Payment> enrichedStream = streamingCDCRows
                .groupingKey(ChangeRecord::key)
                //as of now ordering is not needed due to session window logic below. Code from https://docs.hazelcast.com/hazelcast/5.5/pipelines/cdc-join
                // .mapStateful(
                //         TimeUnit.SECONDS.toMillis(10),
                //         () -> new LongAccumulator(0),
                //         (lastSequence, key, record) -> {
                //             long sequence = record.sequenceValue();
                //             if (lastSequence.get() < sequence) {
                //                 lastSequence.set(sequence);
                //                 return record;
                //             }
                //             return null;
                //         },
                //         (TriFunction<LongAccumulator, RecordPart, Long, ChangeRecord>) (sequence, recordPart,
                //                 aLong) -> null)

                //collect all records in a session maybe caused by burst of updates. Window will close after 100 milliseconds of inactivity
                //using a tumbling window will ALWAYS wait for a fix time whereas as session progresses when no data arrives for 100 ms
                .window(WindowDefinition.session(TimeUnit.MILLISECONDS.toMillis(100)))
                .distinct().setName("Pick any one for the key")
                .mapUsingService(jdbcServiceFactory, (conn, record)->{
                    //FIXME make it async and batched maybe
                    try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM inventory.payments WHERE mandt = ? AND lgnum = ? AND lqnum = ?")) {
                        Map<String, Object> map = record.key().toMap();
                        stmt.setLong(1, (Integer) map.get("mandt"));
                        stmt.setLong(2, (Integer) map.get("lgnum"));
                        stmt.setLong(3, (Integer) map.get("lqnum"));
                        ResultSet rs = stmt.executeQuery();
                        if (rs.next()) {
                            return new Payment(rs.getLong("paymentId"), rs.getString("sourceAccountNo"), rs.getString("targetAccountNo"), rs.getString("title"), rs.getInt("mandt"), rs.getInt("lgnum"), rs.getInt("lqnum"));
                        }else{
                            return new Payment((Integer)map.get("mandt"), (Integer)map.get("lgnum"), (Integer)map.get("lqnum"));
                        }
                    }} )
                .setName("Repopulated from source");
        // DELETE only
        enrichedStream.filter(pmt -> pmt.isDeleted()).setName("Deleted Records")
                    .writeTo(Sinks.jdbc("DELETE FROM inventory.target WHERE mandt = ? AND lgnum = ? AND lqnum = ?", 
                        DataConnectionRef.dataConnectionRef("my-mysql-database")
                        , (stmt, record) -> {
                        stmt.setLong(1, record.getMandt());
                        stmt.setLong(2, record.getLgnum());
                        stmt.setLong(3, record.getLqnum());
                    })).setName("Delete target row");
        // INSERT only
        enrichedStream.filter(pmt -> !pmt.isDeleted()).setName("Upserted Records")
        //merge to target table
        .writeTo(Sinks.jdbc("""
                    INSERT INTO inventory.target 
                    (mandt, lgnum, lqnum, paymentId, sourceAccountNo, targetAccountNo, title) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    paymentId = VALUES(paymentId),
                    sourceAccountNo = VALUES(sourceAccountNo),
                    targetAccountNo = VALUES(targetAccountNo),
                    title = VALUES(title)
                """,
         
            DataConnectionRef.dataConnectionRef("my-mysql-database")
            , (stmt, record) -> {
            stmt.setInt(1, record.getMandt());
            stmt.setInt(2, record.getLgnum());
            stmt.setInt(3, record.getLqnum());
            stmt.setLong(4, record.getPaymentId());
            stmt.setString(5, record.getSourceAccountNo());
            stmt.setString(6, record.getTargetAccountNo());
            stmt.setString(7, record.getTitle());
        })).setName("Upsert target row");

        
        return pipeline;
    }

    private static void getHashjoinBranch(MySQLContainer<?> mySqlContainer, Pipeline pipeline,
            StreamStage<ChangeRecord> streamingCDCRows) {
        //following option needs master data to be in hazelcast which may not be memory efficient

        // in another branch join with database
        StreamStage<Payment> streamingPayments = streamingCDCRows
                .map(changeRecord -> changeRecord.nonNullValue().toObject(Payment.class)).setName("Payment Objects");
        // enrich that data by joining with master data
        BatchStage<Tuple2<Long, String>> mandt = pipeline.readFrom(Sources.jdbc(
                mySqlContainer.getJdbcUrl() + "?user=" + mySqlContainer.getUsername() + "&password="
                        + mySqlContainer.getPassword(),
                "SELECT * FROM inventory.mandt",
                rs -> tuple2(rs.getLong(1), rs.getString(2))));
        streamingPayments
                .hashJoin(mandt, JoinClause.onKeys(Payment::getMandt, mandtRow -> mandtRow.f0()), (pmt, mandtRow) -> {
                    pmt.setMandtname(mandtRow.f1());
                    return pmt;
                }).setName("join payments and mandt")
                .writeTo(Sinks.logger());
    }

    /*
     * Create a Hazelcast instance and configure it
     */
    static HazelcastInstance getHz(MySQLContainer<?> mySqlContainer) throws IOException {
        Config config = Config.load();
        config.getJetConfig().setEnabled(true);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "7");
        config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "log4j2");
        String path = System.getProperty("user.home") + "/hazelcast/v7expiring.license";
        String key = Files.readString(Paths.get(path));
        config.setLicenseKey(key);
        createMapLoaders(config, mySqlContainer);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        return hz;
    }

    /**
     * For the 3 Master tables we will create three MapLoaders (cache) that will
     * cache limited
     * data from the database. This is to avoid loading all the data into memory.
     */
    private static void createMapLoaders(Config hzConfig, MySQLContainer<?> mysqlContainer) {
        // create the data-connection
        hzConfig.addDataConnectionConfig(
                new DataConnectionConfig("my-mysql-database")
                        .setType("JDBC")
                        .setProperty("jdbcUrl", mysqlContainer.getJdbcUrl())
                        .setProperty("user", mysqlContainer.getUsername())
                        .setProperty("password", mysqlContainer.getPassword())
                        .setShared(true));
        
        // limit number of records per Map to 1000
        EvictionConfig evictionConfig = new EvictionConfig().setSize(1000).setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE);

        // create mandt map loader
        MapConfig mandtMapConfig = new MapConfig("mandt");
        mandtMapConfig.setEvictionConfig(evictionConfig);
        MapStoreConfig mandtMapStoreConfig = new MapStoreConfig();
        mandtMapStoreConfig.setClassName("com.hazelcast.mapstore.GenericMapLoader");
        mandtMapStoreConfig.setProperty("data-connection-ref", "my-mysql-database");
        mandtMapStoreConfig.setProperty("id-column", "mandtid");
        mandtMapStoreConfig.setProperty("external-name", "inventory.mandt");
        mandtMapConfig.setMapStoreConfig(mandtMapStoreConfig);
        hzConfig.addMapConfig(mandtMapConfig);

        // create lgnum map loader
        MapConfig lgnumMapConfig = new MapConfig("lgnum");
        lgnumMapConfig.setEvictionConfig(evictionConfig);
        MapStoreConfig lgnumMapStoreConfig = new MapStoreConfig();
        lgnumMapStoreConfig.setClassName("com.hazelcast.mapstore.GenericMapLoader");
        lgnumMapStoreConfig.setProperty("data-connection-ref", "my-mysql-database");
        lgnumMapStoreConfig.setProperty("id-column", "lgnumid");
        lgnumMapStoreConfig.setProperty("external-name", "inventory.lgnum");
        lgnumMapConfig.setMapStoreConfig(lgnumMapStoreConfig);
        hzConfig.addMapConfig(lgnumMapConfig);

        // create lqnum map loader
        MapConfig lqnumMapConfig = new MapConfig("lqnum");
        lqnumMapConfig.setEvictionConfig(evictionConfig);
        MapStoreConfig lqnumMapStoreConfig = new MapStoreConfig();
        lqnumMapStoreConfig.setClassName("com.hazelcast.mapstore.GenericMapLoader");
        lqnumMapStoreConfig.setProperty("data-connection-ref", "my-mysql-database");
        lqnumMapStoreConfig.setProperty("id-column", "lqnumid");
        lqnumMapStoreConfig.setProperty("external-name", "inventory.lqnum");
        lqnumMapConfig.setMapStoreConfig(lqnumMapStoreConfig);
        hzConfig.addMapConfig(lqnumMapConfig);
    }

    static void insertPayment(MySQLContainer<?> container, Payment payment) {
        RandomUtils randomUtils = RandomUtils.insecure();
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                PreparedStatement stmt = conn.prepareStatement(
                        """
                                    INSERT INTO inventory.payments(paymentId, sourceAccountNo, targetAccountNo, title, mandt, lgnum, lqnum)
                                    VALUES (?, ?, ?, ?, ?, ?, ?);
                                """)) {
            stmt.setLong(1, payment.getPaymentId());
            stmt.setString(2, payment.getSourceAccountNo());
            stmt.setString(3, payment.getTargetAccountNo());
            stmt.setString(4, payment.getTitle());
            // insert a random integer between 1 and 5 for mandt, lgnum, and lqnum
            stmt.setLong(5, randomUtils.randomInt(1, UPPER_LIMIT));
            stmt.setLong(6, randomUtils.randomInt(1, UPPER_LIMIT));
            stmt.setLong(7, randomUtils.randomInt(1, UPPER_LIMIT));
            stmt.execute();
        } catch (SQLIntegrityConstraintViolationException e) {
            // ignore duplicate key exception
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * We have one table on which we run CDC and three master data tables. This
     * function
     * creates the three tables and inserts UPPER_LIMIT records into each master
     * table.
     */
    static void createTablesAndMasterData(MySQLContainer<?> container) {
        // create payments table
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                Statement stmt = conn.createStatement()) {

            stmt.execute("""
                            CREATE TABLE inventory.payments (
                                paymentId BIGINT NOT NULL,
                                sourceAccountNo VARCHAR(255) NOT NULL,
                                targetAccountNo VARCHAR(255) NOT NULL,
                                title VARCHAR(255) NOT NULL,
                                mandt BIGINT NOT NULL,
                                lgnum BIGINT NOT NULL,
                                lqnum BIGINT NOT NULL,
                                PRIMARY KEY (mandt, lgnum, lqnum)
                            );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // create target table which is copy of payments table
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                Statement stmt = conn.createStatement()) {

            stmt.execute("""
                            CREATE TABLE inventory.target (
                                paymentId BIGINT NOT NULL,
                                sourceAccountNo VARCHAR(255) NOT NULL,
                                targetAccountNo VARCHAR(255) NOT NULL,
                                title VARCHAR(255) NOT NULL,
                                mandt BIGINT NOT NULL,
                                lgnum BIGINT NOT NULL,
                                lqnum BIGINT NOT NULL,
                                PRIMARY KEY (mandt, lgnum, lqnum)
                            );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // create MANDT table
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                Statement stmt = conn.createStatement()) {

            stmt.execute("""
                    CREATE TABLE inventory.mandt (
                        mandtid BIGINT NOT NULL PRIMARY KEY,
                        mandtname VARCHAR(255) NOT NULL
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // insert five records into mandt table with id 1 to 5
        for (int i = 1; i <= UPPER_LIMIT; i++) {
            try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                    container.getPassword());
                    PreparedStatement stmt = conn.prepareStatement(
                            """
                                            INSERT INTO inventory.mandt(mandtid, mandtname)
                                            VALUES (?, ?);
                                    """)) {
                stmt.setLong(1, i);
                stmt.setString(2, "mandt" + i);
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        // create lgnum table
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                Statement stmt = conn.createStatement()) {

            stmt.execute("""
                    CREATE TABLE inventory.lgnum (
                        lgnumid BIGINT NOT NULL PRIMARY KEY,
                        lgnumname VARCHAR(255) NOT NULL
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // insert five records into lgnum table with id 1 to 5
        for (int i = 1; i <= UPPER_LIMIT; i++) {
            try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                    container.getPassword());
                    PreparedStatement stmt = conn.prepareStatement(
                            """
                                            INSERT INTO inventory.lgnum(lgnumid, lgnumname)
                                            VALUES (?, ?);
                                    """)) {
                stmt.setLong(1, i);
                stmt.setString(2, "lgnum" + i);
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        // create lqnum table
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
                Statement stmt = conn.createStatement()) {

            stmt.execute("""
                    CREATE TABLE inventory.lqnum (
                        lqnumid BIGINT NOT NULL PRIMARY KEY,
                        lqnumname VARCHAR(255) NOT NULL
                    );
                    """);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // insert five records into lqnum table with id 1 to 5
        for (int i = 1; i <= UPPER_LIMIT; i++) {
            try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                    container.getPassword());
                    PreparedStatement stmt = conn.prepareStatement(
                            """
                                            INSERT INTO inventory.lqnum(lqnumid, lqnumname)
                                            VALUES (?, ?);
                                    """)) {
                stmt.setLong(1, i);
                stmt.setString(2, "lqnum" + i);
                stmt.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

}