package com.gkalogiros.kafka;

import kafka.server.KafkaConfig$;
import kafka.utils.MockTime;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.streams.integration.utils.KafkaEmbedded;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.util.Properties;

class KafkaTestingCluster extends ExternalResource {

    private static final MockTime MOCK_TIME = new MockTime();

    private EmbeddedZooKeeper zookeeper;
    private KafkaEmbedded broker;

    @Override
    protected void before() throws Exception {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    String getKafkaServers() {
        return broker.brokerList();
    }

    private void start() throws Exception {

        zookeeper = new EmbeddedZooKeeper();

        broker = new KafkaEmbedded(new KafkaBrokerProperties(zookeeper.connectString()), MOCK_TIME);
    }

    private void stop() {

        if (broker != null) {
            broker.stop();
        }

        try {
            if (zookeeper != null) {
                zookeeper.stop();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class KafkaBrokerProperties extends Properties {

        private static final int DEFAULT_BROKER_PORT = 0;
        private static final int ZK_SESSION_TIMEOUT_MS = 30 * 1000;
        private static final int ZK_CONNECTION_TIMEOUT_MS = 60 * 1000;
        private static final int ZK_GROUP_MINIMUM_SESSION_TIMEOUT_MS = 0;
        private static final int TOPIC_PARTITIONS = 1;
        private static final short TOPIC_REPLICATION = 1;
        private static final long LOG_CLEANER_BUFFER_SIZE = 2 * 1024 * 1024L;

        private KafkaBrokerProperties(final String zookeeperConnectString) {
            super();
            put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeperConnectString);
            put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), ZK_SESSION_TIMEOUT_MS);
            put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
            put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), ZK_CONNECTION_TIMEOUT_MS);
            put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
            put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), LOG_CLEANER_BUFFER_SIZE);
            put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), ZK_GROUP_MINIMUM_SESSION_TIMEOUT_MS);
            put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), TOPIC_REPLICATION);
            put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), TOPIC_PARTITIONS);
            put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
        }
    }

    private static class EmbeddedZooKeeper {

        private final TestingServer server;

        private EmbeddedZooKeeper() throws Exception {
            this.server = new TestingServer();
        }

        private void stop() throws IOException {
            server.close();
        }

        private String connectString() {
            return server.getConnectString();
        }
    }
}