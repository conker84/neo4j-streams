package streams

import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.Log

abstract class StreamsEventSink(private val config: Config,
                                private val queryExecution: StreamsEventSinkQueryExecution,
                                private val streamsTopicService: StreamsTopicService,
                                private val log: Log) {

    private var streamsEventSinkRepository: StreamsEventConsumerRepository? = null

    abstract fun stop()

    abstract fun start()

    abstract fun getEventConsumerFactory(): StreamsEventConsumerFactory

    abstract fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper

    fun getEventSinkRepository(): StreamsEventConsumerRepository {
        if (streamsEventSinkRepository == null) {
            streamsEventSinkRepository = StreamsEventConsumerRepository(getEventConsumerFactory())
        }
        return streamsEventSinkRepository!!
    }

}

enum class ConsumerStatus { INITIALIZED, RUNNING, STOPPED }

abstract class StreamsEventConsumer<T>(private val consumer: T, config: StreamsSinkConfiguration, private val log: Log) {

    abstract fun stop()

    abstract fun withTopics(topics: Set<String>): StreamsEventConsumer<T>

    abstract fun start()

    abstract fun read(): Map<String, List<Any>>?

    abstract fun status(): ConsumerStatus

}

abstract class StreamsEventConsumerFactory {
    abstract fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer<*>
}

object StreamsEventSinkFactory {
    fun getStreamsEventSink(config: Config, streamsQueryExecution: StreamsEventSinkQueryExecution,
                            streamsTopicService: StreamsTopicService, log: Log): StreamsEventSink {
        return Class.forName(config.raw.getOrDefault("streams.sink", "streams.kafka.KafkaEventSink"))
                .getConstructor(Config::class.java,
                        StreamsEventSinkQueryExecution::class.java,
                        StreamsTopicService::class.java,
                        Log::class.java)
                .newInstance(config, streamsQueryExecution, streamsTopicService, log) as StreamsEventSink
    }
}

abstract class StreamsEventSinkConfigMapper(val baseConfiguration: Map<String, String>, private val mapping: Map<String, String>) {
    abstract fun convert(config: Map<String, String>): Map<String, String>
}
