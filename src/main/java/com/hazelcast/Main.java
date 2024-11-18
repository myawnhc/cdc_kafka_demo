package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.jet.cdc.Operation;
import com.hazelcast.enterprise.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import org.apache.commons.lang3.RandomUtils;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.vector.jet.VectorTransforms.mapUsingVectorSearch;

@SuppressWarnings("SqlResolve")
public class Main {
    public static final DockerImageName DEBEZIUM_MYSQL_IMAGE
            = DockerImageName.parse("debezium/example-mysql:2.7.1.Final")
                             .asCompatibleSubstituteFor("mysql");

    enum Classifier {
        LEGIT,
        SUSPICIOUS
    }
    record ProcessedPayment (Classifier classifier, Payment payment) {
        @Override
        public String toString() {
            return String.format("%-10s :: %s", classifier, payment);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //noinspection resource
        try (var container = new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw")
                .withExposedPorts(3306)
                .withNetworkAliases("inputDatabase")) {
            container.start();

            try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
                 Statement stmt = conn.createStatement()) {
                stmt.execute("""
                CREATE TABLE inventory.payments (
                    paymentId BIGINT NOT NULL primary key,
                    sourceAccountNo VARCHAR(255) NOT NULL,
                    targetAccountNo VARCHAR(255) NOT NULL,
                    title VARCHAR(255) NOT NULL
                );
                """);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Config config = Config.load();
            VectorCollectionConfig vcc = new VectorCollectionConfig("processedPayments")
                    .addVectorIndexConfig(new VectorIndexConfig("processedPayments-fraud", Metric.COSINE, 384,
                            40, 100, false));
            config.addVectorCollectionConfig(vcc);
            config.getJetConfig().setEnabled(true);
            config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "7");
            config.setProperty(ClusterProperty.LOGGING_TYPE.getName(), "log4j2");
            HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

            VectorCollection<Long, ProcessedPayment> processedPayments = VectorCollection.getCollection(hz, "processedPayments");

            insertTrainingData(processedPayments);

            Pipeline pipeline = Pipeline.create();
            var source = MySqlCdcSources.mysql("payments")
                                        .setDatabaseAddress(container.getHost(), container.getMappedPort(MySQLContainer.MYSQL_PORT))
                                        .setDatabaseCredentials("debezium", "dbz")
                                        .setTableIncludeList("inventory.payments")
                                        .build();
            var classifiedPayments = pipeline.readFrom(source)
                                             .withIngestionTimestamps()
                                             .filter(changeRecord -> changeRecord.operation() != Operation.UNSPECIFIED)
                                             .map(changeRecord -> changeRecord.nonNullValue().toObject(Payment.class))
                                             .apply(mapUsingVectorSearch("processedPayments", SearchOptions.of(10, true, true),
                                                     Payment::toVector,
                                                     Main::classify));
            classifiedPayments.writeTo(Sinks.logger());
            classifiedPayments
                    .map(procPay -> tuple2(procPay.payment.paymentId(), procPay))
                    .writeTo(Sinks.map("results"));

            hz.getJet().newJob(pipeline);

            Thread.sleep(Duration.ofSeconds(3));

            insertPayment(container, Payment.generatePayment(Classifier.SUSPICIOUS));
            insertPayment(container, Payment.generatePayment(Classifier.LEGIT));

            RandomUtils randomUtils = RandomUtils.insecure();
            //noinspection InfiniteLoopStatement
            while (true) {
                Classifier classifier = randomUtils.randomBoolean() ? Classifier.LEGIT : Classifier.SUSPICIOUS;
                insertPayment(container, Payment.generatePayment(classifier));
                Thread.sleep(Duration.ofSeconds(2));
            }
        }
    }

    private static void insertPayment(MySQLContainer<?> container, Payment payment) {
        try (Connection conn = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     """
                INSERT INTO inventory.payments(paymentId, sourceAccountNo, targetAccountNo, title)
                VALUES (?, ?, ?, ?);
                """
             )) {
            stmt.setLong(1, payment.paymentId());
            stmt.setString(2, payment.sourceAccountNo());
            stmt.setString(3, payment.targetAccountNo());
            stmt.setString(4, payment.title());
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void insertTrainingData(VectorCollection<Long, ProcessedPayment> processedPayments) {
        var sus = List.of(
                new Payment("0001mafia", "0002mafia", "totally not from robbery"),
                new Payment("0002mafia", "0001mafia", "robbery"),
                new Payment("000111111", "0002mafia", "extortion"),
                new Payment("00011458974813", "0001mafia", "extortion jan-jul"),
                new Payment("54867324894123", "0003mafia", "extortion jan-jul"),
                new Payment("3229462", "0002mafia", "mafia")
        );
        var legit = List.of(
                new Payment("98894561567894231", "8423178492631", "invoice 1234"),
                new Payment("612894123189562", "1854513589", "salary"),
                new Payment("000111111", "0002mafia", "extortion"),
                new Payment("00011458974813", "54867324894123", "pizza"),
                new Payment("54867324894123", "00011458974813", "my tribute")
        );
        for (Payment payment : sus) {
            processedPayments.putAsync(payment.paymentId(), VectorDocument.of(new ProcessedPayment(Classifier.SUSPICIOUS, payment),
                    payment.toVector()));
        }
        for (Payment payment : legit) {
            processedPayments.putAsync(payment.paymentId(), VectorDocument.of(new ProcessedPayment(Classifier.LEGIT, payment),
                    payment.toVector()));
        }
    }

    private static ProcessedPayment classify(Payment payment, SearchResults<Long, ProcessedPayment> searchResults) {
        float[] scores = { 0, 0 };
        searchResults.results().forEachRemaining(result -> {
            ProcessedPayment value = result.getValue();
            assert value != null;
            scores[value.classifier().ordinal()] += result.getScore();
        });
        Classifier classifier = scores[0] > scores[1] ? Classifier.LEGIT : Classifier.SUSPICIOUS;
        return new ProcessedPayment(classifier, payment);
    }

}