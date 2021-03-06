package streams.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.Test
import org.neo4j.kernel.configuration.Config
import streams.StreamsSinkConfiguration
import streams.StreamsSinkConfigurationTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KafkaSinkConfigurationTest {

    @Test
    fun shouldReturnDefaultConfiguration() {
        val default = KafkaSinkConfiguration()
        StreamsSinkConfigurationTest.testDefaultConf(default.streamsSinkConfiguration)

        assertEquals("localhost:2181", default.zookeeperConnect)
        assertEquals("localhost:9092", default.bootstrapServers)
        assertEquals("neo4j", default.groupId)
        assertEquals("earliest", default.autoOffsetReset)
        assertEquals(emptyMap(), default.extraProperties)
    }

    @Test
    fun shouldReturnConfigurationFromMap() {
        val pollingInterval = "10"
        val topic = "topic-neo"
        val topicKey = "streams.sink.topic.cypher.$topic"
        val topicValue = "MERGE (n:Label{ id: event.id }) "
        val zookeeper = "zookeeper:2181"
        val bootstrap = "bootstrap:9092"
        val group = "foo"
        val autoOffsetReset = "latest"
        val autoCommit = "false"
        val config = Config.builder()
                .withSetting("streams.sink.polling.interval", pollingInterval)
                .withSetting(topicKey, topicValue)
                .withSetting("kafka.zookeeper.connect", zookeeper)
                .withSetting("kafka.bootstrap.servers", bootstrap)
                .withSetting("kafka.auto.offset.reset", autoOffsetReset)
                .withSetting("kafka.enable.auto.commit", autoCommit)
                .withSetting("kafka.group.id", group)
                .build()
        val expectedMap = mapOf("zookeeper.connect" to zookeeper, "bootstrap.servers" to bootstrap,
                "auto.offset.reset" to autoOffsetReset, "enable.auto.commit" to autoCommit, "group.id" to group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.toString())

        val kafkaSinkConfiguration = KafkaSinkConfiguration.from(config)
        StreamsSinkConfigurationTest.testFromConf(kafkaSinkConfiguration.streamsSinkConfiguration, pollingInterval, topic, topicValue)
        assertEquals(emptyMap(), kafkaSinkConfiguration.extraProperties)
        assertEquals(zookeeper, kafkaSinkConfiguration.zookeeperConnect)
        assertEquals(bootstrap, kafkaSinkConfiguration.bootstrapServers)
        assertEquals(autoOffsetReset, kafkaSinkConfiguration.autoOffsetReset)
        assertEquals(group, kafkaSinkConfiguration.groupId)
        val resultMap = kafkaSinkConfiguration
                .asProperties()
                .map { it.key.toString() to it.value.toString() }
                .associateBy({ it.first }, { it.second })
        assertEquals(expectedMap, resultMap)

        val streamsConfig = StreamsSinkConfiguration.from(config)
        assertEquals(pollingInterval.toLong(), streamsConfig.sinkPollingInterval)
        assertTrue { streamsConfig.topics.cypherTopics.containsKey(topic) }
        assertEquals(topicValue, streamsConfig.topics.cypherTopics[topic])
    }

}