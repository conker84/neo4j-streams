package streams

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.test.TestGraphDatabaseFactory
import streams.kafka.KafkaSinkConfiguration
import streams.utils.Neo4jUtils.writeInTxWithGraphDatabaseService
import kotlin.test.assertEquals

class StreamsEventSinkQueryExecutionTest {
    private lateinit var db: GraphDatabaseAPI
    private lateinit var streamsEventSinkQueryExecution: StreamsEventSinkQueryExecution

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase() as GraphDatabaseAPI
        val kafkaConfig = KafkaSinkConfiguration(streamsSinkConfiguration = StreamsSinkConfiguration(cypherTopics = mapOf("shouldWriteCypherQuery" to "MERGE (n:Label {id: event.id})\n" +
                "    ON CREATE SET n += event.properties")))
        val streamsTopicService = StreamsTopicService(db)
        writeInTxWithGraphDatabaseService(db) {
            streamsTopicService.setAllCypherTemplates(kafkaConfig.streamsSinkConfiguration.cypherTopics)
            it.success()
        }
        streamsEventSinkQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db, NullLog.getInstance())
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