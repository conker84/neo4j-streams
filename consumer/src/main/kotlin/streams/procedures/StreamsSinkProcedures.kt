package streams.procedures

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import streams.*
import java.util.stream.Stream

class StreamResult(@JvmField val event: Map<String, *>)

class StreamStatus(@JvmField val topic: String, @JvmField val query: String, @JvmField val status: String)

class StreamsSinkProcedures {

    @JvmField @Context
    var log: Log? = null

    @JvmField @Context
    var db: GraphDatabaseService? = null

    @Procedure(mode = Mode.WRITE, name = "streams.consume")
    @Description("streams.consume(topic, {timeout: <long value>, from: <string>}) YIELD event - Allows to consume custom topics")
    fun consume(@Name("topic") topic: String?,
                @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamResult> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty, no message sent")
            return Stream.empty()
        }

        val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
        val configuration = StreamsSinkProcedures.streamsEventSinkConfigMapper.convert(config = properties)

        val consumer = StreamsSinkProcedures
                .streamsEventConsumerFactory
                .createStreamsEventConsumer(configuration, log!!)
                .withTopics(setOf(topic))
        consumer.start()
        val data = try {
            consumer.read()
        } catch (e: Exception) {
            if (log?.isDebugEnabled!!) {
                log?.error("Error while consuming data", e)
            }
            emptyMap<String, List<Any>>()
        }
        consumer.stop()

        if (log?.isDebugEnabled!!) {
            log?.debug("Data retrieved from topic $topic after ${configuration["streams.sink.polling.interval"]} milliseconds: $data")
        }
        return data?.values?.flatMap { list -> list.map { StreamResult(mapOf("data" to it)) } }?.stream() ?: Stream.empty()
    }

    @Procedure(mode = Mode.READ, name = "streams.consume.status")
    @Description("") // TODO
    fun status(@Name("topic", defaultValue = "") topic: String?): Stream<StreamStatus> {
        checkEnabled()
        if (topic.isNullOrBlank()) {
            val list = streamsEventSinkRepository.status()

            return if (list.isEmpty()) Stream.empty() else list.map {
                val topic = it.getValue("topic")
                val query = if (streamsTopicService.isCDCTopic(topic)) {
                    "CDC"
                } else {
                    streamsTopicService.getCypherTemplate(topic)!!
                }
                val status = it.getValue("status")
                StreamStatus(topic, query, status)
            }.stream()
        } else {
            if (!streamsTopicService.exists(topic)) {
                return Stream.empty()
            }

            val query = if (streamsTopicService.isCDCTopic(topic)) {
                "CDC"
            } else {
                streamsTopicService.getCypherTemplate(topic) ?: ""
            }
            val status = streamsEventSinkRepository.status(topic)!!.toString()
            return Stream.of(StreamStatus(topic, query, status))
        }
    }

    @Procedure(mode = Mode.WRITE, name = "streams.consume.remove")
    @Description("") // TODO
    fun remove(@Name("topic") topic: String?): Stream<StreamStatus> {
        checkEnabled()
        if (topic.isNullOrEmpty() || !streamsTopicService.exists(topic)) {
            log?.info("Topic empty")
            return Stream.empty()
        }
        val query = streamsTopicService.getCypherTemplate(topic) ?: "CDC"
        streamsEventSinkRepository.remove(topic)
        streamsTopicService.remove(topic)
        return Stream.of(StreamStatus(topic, query, "REMOVED"))
    }

    @Procedure(mode = Mode.WRITE, name = "streams.consume.add.cdc")
    @Description("") // TODO
    fun addCDC(@Name("topic") topic: String?,
            @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamStatus> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty")
            return Stream.empty()
        }
        val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
        val configuration = streamsEventSinkConfigMapper.convert(config = properties)

        streamsTopicService.setCDCTopic(topic)
        streamsEventSinkRepository.add(topic, configuration, log!!)
        val status = streamsEventSinkRepository.status(topic)?.toString() ?: ""
        return Stream.of(StreamStatus(topic, "CDC", status))
    }

    @Procedure(mode = Mode.WRITE, name = "streams.consume.add.cypher")
    @Description("") // TODO
    fun addCypher(@Name("topic") topic: String?, @Name("query") query: String?,
            @Name(value = "config", defaultValue = "{}") config: Map<String, Any>?): Stream<StreamStatus> {
        checkEnabled()
        if (topic.isNullOrEmpty()) {
            log?.info("Topic empty")
            return Stream.empty()
        }
        if (query.isNullOrEmpty()) {
            log?.info("Query empty")
            return Stream.empty()
        }
        val properties = config?.mapValues { it.value.toString() } ?: emptyMap()
        val configuration = streamsEventSinkConfigMapper.convert(config = properties)

        streamsTopicService.setCypherTemplate(topic, query)
        streamsEventSinkRepository.add(topic, configuration, log!!)
        val status = streamsEventSinkRepository.status(topic)?.toString() ?: ""
        return Stream.of(StreamStatus(topic, query, status))
    }

    private fun checkEnabled() {
        if (!StreamsSinkProcedures.streamsSinkConfiguration.proceduresEnabled) {
            throw RuntimeException("In order to use the procedure you must set streams.procedures.enabled=true")
        }
    }


    companion object {
        private lateinit var streamsSinkConfiguration: StreamsSinkConfiguration
        private lateinit var streamsEventConsumerFactory: StreamsEventConsumerFactory
        private lateinit var streamsEventSinkConfigMapper: StreamsEventSinkConfigMapper
        private lateinit var streamsEventSinkRepository: StreamsEventConsumerRepository
        private lateinit var streamsTopicService: StreamsTopicService

        fun registerStreamsEventSinkConfigMapper(streamsEventSinkConfigMapper: StreamsEventSinkConfigMapper) {
            this.streamsEventSinkConfigMapper = streamsEventSinkConfigMapper
        }

        fun registerStreamsSinkConfiguration(streamsSinkConfiguration: StreamsSinkConfiguration) {
            this.streamsSinkConfiguration = streamsSinkConfiguration
        }

        fun registerStreamsEventConsumerFactory(streamsEventConsumerFactory: StreamsEventConsumerFactory) {
            this.streamsEventConsumerFactory = streamsEventConsumerFactory
        }

        fun registerStreamsEventSinkRepository(streamsEventSinkRepository: StreamsEventConsumerRepository) {
            this.streamsEventSinkRepository = streamsEventSinkRepository
        }

        fun registerStreamsTopicService(streamsTopicService: StreamsTopicService) {
            this.streamsTopicService = streamsTopicService
        }
    }
}