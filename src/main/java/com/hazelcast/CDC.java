package com.hazelcast;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hazelcast.Main.Classifier;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.jet.cdc.Operation;
import com.hazelcast.enterprise.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.spi.properties.ClusterProperty;

@SuppressWarnings("SqlResolve")
public class CDC {
    private static final int UPPER_LIMIT = 100;
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
        //mvn exec:java -Dexec.mainClass=com.hazelcast.CDC
        // create mysql container
        try (var container = new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw")
                .withExposedPorts(3306)
                .withNetworkAliases("inputDatabase")) {
            container.start();
            
            //create the tables and add master data
            createTablesAndMasterData(container);
            
            //run hazeclast instance
            HazelcastInstance hz = getHz();

            // Properties for Kafka sink
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", "localhost:9092");
            props.setProperty("key.serializer", LongSerializer.class.getCanonicalName());
            props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
            
            //create the pipeline
            Pipeline pipeline = getHashjoinPipeline(container);
            
            //deploy the pipeline
            hz.getJet().newJob(pipeline);

            Thread.sleep(Duration.ofSeconds(3));
            
            //start inserting payments
            insertPayment(container, Payment.generatePayment(Classifier.SUSPICIOUS));
            insertPayment(container, Payment.generatePayment(Classifier.LEGIT));

            RandomUtils randomUtils = RandomUtils.insecure();
            // noinspection InfiniteLoopStatement
            while (true) {
                Classifier classifier = randomUtils.randomBoolean() ? Classifier.LEGIT : Classifier.SUSPICIOUS;
                insertPayment(container, Payment.generatePayment(classifier));
                Thread.sleep(Duration.ofSeconds(2));
            }
        }
    }

    private static Pipeline getHashjoinPipeline(MySQLContainer<?> container) {
        Pipeline pipeline = Pipeline.create();
        var source = MySqlCdcSources.mysql("payments")
                .setDatabaseAddress(container.getHost(), container.getMappedPort(MySQLContainer.MYSQL_PORT))
                .setDatabaseCredentials("debezium", "dbz")
                .setTableIncludeList("inventory.payments")
                .build();
        //get streaming data from CDC
        StreamStage<Payment> streamingPayments = pipeline.readFrom(source)
                .withIngestionTimestamps()
                .filter(changeRecord -> changeRecord.operation() != Operation.UNSPECIFIED)
                .map(changeRecord -> changeRecord.nonNullValue().toObject(Payment.class));
        //enrich that data by joining with master data
        BatchStage<Tuple2<Long, String>> mandt = pipeline.readFrom(Sources.jdbc(
                container.getJdbcUrl()+"?user="+container.getUsername()+"&password="+container.getPassword(),
                "SELECT * FROM inventory.mandt",
                rs -> tuple2(rs.getLong(1), rs.getString(2))));
        streamingPayments
            .hashJoin(mandt, JoinClause.onKeys(Payment::getMandt, mandtRow -> mandtRow.f0()), (pmt, mandtRow) -> {
                pmt.setMandtname(mandtRow.f1()); 
                return pmt;
            })
            .writeTo(Sinks.logger());
        return pipeline;
    }

    private static HazelcastInstance getHz() throws IOException {
        Config config = Config.load();
        config.getJetConfig().setEnabled(true);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "7");
        config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "log4j2");
        String path = System.getProperty("user.home") + "/hazelcast/v7expiring.license";
        String key = Files.readString(Paths.get(path));
        config.setLicenseKey(key);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        return hz;
    }

    private static void insertPayment(MySQLContainer<?> container, Payment payment) {
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

    private static void createTablesAndMasterData(MySQLContainer<?> container) {
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