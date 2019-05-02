package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.test.TestGraphDatabaseFactory
import streams.kafka.KafkaSinkConfiguration
import streams.service.TopicType
import streams.service.Topics
import kotlin.test.assertEquals

class StreamsEventSinkQueryExecutionTest {
    private lateinit var db: GraphDatabaseService
    private lateinit var streamsEventSinkQueryExecution: StreamsEventSinkQueryExecution

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase()
        val kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(topics = Topics(cypherTopics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
                "    ON CREATE SET n += event.properties"))))
        val streamsTopicService = StreamsTopicService(db as GraphDatabaseAPI)
        streamsTopicService.setAllCypherTemplates(kafkaConfig.streamsSinkConfiguration.topics.cypherTopics)
        streamsEventSinkQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db as GraphDatabaseAPI,
                NullLog.getInstance(), emptyMap())
    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun shouldWriteCypherQuery() {
        streamsEventSinkQueryExecution.writeForTopic("shouldWriteCypherQuery", listOf(mapOf("id" to "1", "properties" to mapOf("a" to 1)),
                mapOf("id" to "2", "properties" to mapOf("a" to 1))))

        db.execute("MATCH (n:Label) RETURN count(n) AS count").columnAs<Long>("count").use {
            assertEquals(2, it.next())
        }

    }

}