package integrations

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.*
import org.junit.rules.TestName
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.testcontainers.containers.KafkaContainer
import streams.procedures.StreamsSinkProcedures
import streams.serialization.JSONUtils
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


class StreamsSinkProceduresIT {
    companion object {
        /**
         * Kafka TestContainers uses Confluent OSS images.
         * We need to keep in mind which is the right Confluent Platform version for the Kafka version this project uses
         *
         * Confluent Platform | Apache Kafka
         *                    |
         * 4.0.x	          | 1.0.x
         * 4.1.x	          | 1.1.x
         * 5.0.x	          | 2.0.x
         *
         * Please see also https://docs.confluent.io/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
         */
        private const val confluentPlatformVersion = "4.0.2"
        @ClassRule
        @JvmField
        val kafka = KafkaContainer(confluentPlatformVersion)
    }

    private lateinit var db: GraphDatabaseAPI

    private val cypherQueryTemplate = "MERGE (n:Label {id: event.id}) ON CREATE SET n += event.properties"

    private val topics = listOf("shouldWriteCypherQuery")

    @Rule
    @JvmField
    var testName = TestName()

    private val EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX = "WithNoTopicLoaded"
    private val EXCLUDE_SINK_METHOD_SUFFIX = "WithSinkDisabled"

    private val kafkaProperties = Properties()

    private lateinit var kafkaProducer: KafkaProducer<String, ByteArray>

    // Test data
    private val dataProperties = mapOf("prop1" to "foo", "bar" to 1)
    private val data = mapOf("id" to 1, "properties" to dataProperties)


    @Before
    fun setUp() {
        var graphDatabaseBuilder = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("kafka.bootstrap.servers", kafka.bootstrapServers)
        if (!testName.methodName.endsWith(EXCLUDE_LOAD_TOPIC_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.sink.topic.cypher.shouldWriteCypherQuery", cypherQueryTemplate)
        }
        if (testName.methodName.endsWith(EXCLUDE_SINK_METHOD_SUFFIX)) {
            graphDatabaseBuilder.setConfig("streams.sink.enabled", "false")
        }
        db = graphDatabaseBuilder.newGraphDatabase() as GraphDatabaseAPI
        db.dependencyResolver.resolveDependency(Procedures::class.java)
                .registerProcedure(StreamsSinkProcedures::class.java, true)

        kafkaProperties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        kafkaProperties["zookeeper.connect"] = kafka.envMap["KAFKA_ZOOKEEPER_CONNECT"]
        kafkaProperties["group.id"] = "neo4j"
        kafkaProperties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        kafkaProperties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java

        kafkaProducer = KafkaProducer(kafkaProperties)
    }

    @After
    fun tearDown() {
        db.shutdown()
        kafkaProducer.close()
    }


    @Test
    fun shouldConsumeDataFromProcedureWithSinkDisabled() {
        val topic = "bar"
        testProcedure(topic)
    }

    @Test
    fun shouldConsumeDataFromProcedure() {
        val topic = "foo"
        testProcedure(topic)
    }

    private fun testProcedure(topic: String) {
        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(data))
        kafkaProducer.send(producerRecord).get()
        val result = db.execute("CALL streams.consume('$topic', {timeout: 5000}) YIELD event RETURN event")
        assertTrue { result.hasNext() }
        val resultMap = result.next()
        assertTrue { resultMap.containsKey("event") }
        assertNotNull(resultMap["event"], "should contain event")
        val event = resultMap["event"] as Map<String, Any?>
        assertEquals(data, event)
    }

    @Test
    fun shouldTimeout() {
        val result = db.execute("CALL streams.consume('foo1', {timeout: 2000}) YIELD event RETURN event")
        assertFalse { result.hasNext() }
    }

}