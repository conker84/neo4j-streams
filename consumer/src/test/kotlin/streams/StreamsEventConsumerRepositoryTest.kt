package streams

import org.easymock.EasyMockSupport
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.NullLog
import org.neo4j.test.TestGraphDatabaseFactory
import streams.kafka.KafkaEventSink
import kotlin.test.assertEquals

class StreamsEventConsumerRepositoryTest: EasyMockSupport() {

    private lateinit var db: GraphDatabaseAPI
    private lateinit var streamsTopicService: StreamsTopicService
    private lateinit var streamsEventConsumerRepository: StreamsEventConsumerRepository
    private lateinit var streamsEventConsumerFactory: StreamsEventConsumerFactory


    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase() as GraphDatabaseAPI
        streamsTopicService = StreamsTopicService(db)
        val queryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db, NullLog.getInstance())
        streamsEventConsumerFactory = KafkaEventSink(Config.defaults(), queryExecution, streamsTopicService, NullLog.getInstance()).getEventConsumerFactory()

    }

    @After
    fun tearDown() {
        db.shutdown()
    }

    @Test
    fun `should start a consumer`() {
        // given
        streamsEventConsumerRepository = StreamsEventConsumerRepository(streamsEventConsumerFactory)
        val topic = "new-topic"

        // when
        streamsEventConsumerRepository.add(topic, emptyMap(), NullLog.getInstance())

        // then
        assertEquals(setOf(topic), streamsEventConsumerRepository.topics())
        assertEquals(ConsumerStatus.RUNNING, streamsEventConsumerRepository.get(topic)?.status())
    }

    @Test
    fun `should stop a consumer`() {
        // given
        streamsEventConsumerRepository = StreamsEventConsumerRepository(streamsEventConsumerFactory)
        val topic = "new-topic"
        streamsEventConsumerRepository.add(topic, emptyMap(), NullLog.getInstance())

        // when
        streamsEventConsumerRepository.stop(topic)

        // then
        assertEquals(setOf(topic), streamsEventConsumerRepository.topics())
        assertEquals(ConsumerStatus.STOPPED, streamsEventConsumerRepository.get(topic)?.status())
    }

    @Test
    fun `should remove a consumer`() {
        // given
        streamsEventConsumerRepository = StreamsEventConsumerRepository(streamsEventConsumerFactory)
        val topic = "new-topic"
        streamsEventConsumerRepository.add(topic, emptyMap(), NullLog.getInstance())

        // when
        streamsEventConsumerRepository.remove(topic)

        // then
        assertEquals(emptySet(), streamsEventConsumerRepository.topics())
    }
}