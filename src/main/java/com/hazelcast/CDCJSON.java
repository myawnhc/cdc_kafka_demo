package com.hazelcast;

import java.time.Duration;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.hazelcast.Main.Classifier;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;

@SuppressWarnings("SqlResolve")
public class CDCJSON {
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
        // mvn clean install exec:java -Dexec.mainClass=com.hazelcast.CDCJSON
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
            CDC.createTablesAndMasterData(mysqlContainer);

            // run hazeclast instance
            HazelcastInstance hz = CDC.getHz(mysqlContainer);

            // create the pipeline
            Pipeline pipeline = getPipeline(mysqlContainer, kafkaContainer);

            // deploy the pipeline
            hz.getJet().newJob(pipeline);

            Thread.sleep(Duration.ofSeconds(3));

            // start inserting payments
            CDC.insertPayment(mysqlContainer, Payment.generatePayment(Classifier.SUSPICIOUS));
            CDC.insertPayment(mysqlContainer, Payment.generatePayment(Classifier.LEGIT));

            RandomUtils randomUtils = RandomUtils.insecure();
            // noinspection InfiniteLoopStatement
            while (true) {
                Classifier classifier = randomUtils.randomBoolean() ? Classifier.LEGIT : Classifier.SUSPICIOUS;
                CDC.insertPayment(mysqlContainer, Payment.generatePayment(classifier));
                Thread.sleep(Duration.ofSeconds(2));
            }
        } finally {
            kafkaContainer.stop();
        }
    }

    private static Pipeline getPipeline(MySQLContainer<?> mySqlContainer, KafkaContainer kafkaContainer) {

        Pipeline pipeline = Pipeline.create();
        var source = MySqlCdcSources.mysql("payments")
                .setDatabaseAddress(mySqlContainer.getHost(), mySqlContainer.getMappedPort(MySQLContainer.MYSQL_PORT))
                .setDatabaseCredentials("debezium", "dbz")
                .setTableIncludeList("inventory.payments")
                .json()//this will push original data to Hazelcast. Alternative is changeRecord()
                .build();
        // Properties for Kafka sink
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
        props.setProperty("value.serializer", StringSerializer.class.getCanonicalName());

        // get streaming data from CDC
        StreamStage<Entry<String, String>> cdcSource = pipeline.readFrom(source)
                .withIngestionTimestamps();
        cdcSource.writeTo(Sinks.logger());
        cdcSource.writeTo(KafkaSinks.kafka(props, "payments"));

        return pipeline;
    }

}