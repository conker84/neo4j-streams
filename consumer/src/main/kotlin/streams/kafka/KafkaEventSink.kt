package streams.kafka

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.Log
import streams.*
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils
import java.time.Duration
import java.time.temporal.TemporalUnit
import java.util.concurrent.TimeUnit

class KafkaEventSink(private val config: Config,
                     private val queryExecution: StreamsEventSinkQueryExecution,
                     private val streamsTopicService: StreamsTopicService,
                     private val log: Log): StreamsEventSink(config, queryExecution, streamsTopicService, log) {

    private lateinit var job: Job

    private val streamsConfigMap = config.raw.filterKeys {
        it.startsWith("kafka.") || (it.startsWith("streams.") && !it.startsWith("streams.sink.topic.cypher."))
    }.toMap()

    private val mappingKeys = mapOf("timeout" to "streams.sink.polling.interval",
            "from" to "kafka.auto.offset.reset")

    override fun getEventConsumerFactory(): StreamsEventConsumerFactory {
        return object: StreamsEventConsumerFactory() {
            override fun createStreamsEventConsumer(config: Map<String, String>, log: Log): StreamsEventConsumer<*> {
                val kafkaConfig = KafkaSinkConfiguration.from(config)
                val kafkaConsumer = KafkaConsumer<String, ByteArray>(kafkaConfig.asProperties())
                return KafkaEventConsumer(kafkaConsumer, kafkaConfig.streamsSinkConfiguration, log)
            }
        }
    }

    override fun start() {
        val streamsConfig = StreamsSinkConfiguration.from(config)
        if (!streamsConfig.enabled) {
            return
        }
        log.info("Starting the Kafka Sink")
        this.job = createJob()
        log.info("Kafka Sink started")
    }

    override fun stop() = runBlocking {
        log.info("Stopping Sink daemon Job")
        try {
            job.cancelAndJoin()
        } catch (e : UninitializedPropertyAccessException) { /* ignoring this one only */ }
    }

    override fun getEventSinkConfigMapper(): StreamsEventSinkConfigMapper {
        return object: StreamsEventSinkConfigMapper(streamsConfigMap, mappingKeys) {
            override fun convert(config: Map<String, String>): Map<String, String> {
                val props = streamsConfigMap
                        .toMutableMap()
                props += config.mapKeys { mappingKeys.getOrDefault(it.key, it.key) }
                return props
            }

        }
    }

    private fun createJob(): Job {
        log.info("Creating Sink daemon Job")
        return GlobalScope.launch(Dispatchers.IO) {
            while (isActive) {
                getEventSinkRepository().forEach { topic, eventConsumer ->
                    try {
                        if (!isActive) { // immediately stop the job
                            return@forEach
                        }
                        val data = eventConsumer.read()
                        data?.forEach {
                            if (log.isDebugEnabled) {
                                log.debug("Reading data from topic ${it.key}, with data ${it.value}")
                            }
                            queryExecution.writeForTopic(it.key, it.value)
                        }
                    } catch (e: Throwable) {
                        val message = e.message ?: "Generic error, please check the stack trace: "
                        log.error(message, e)
                        eventConsumer.stop()
                    }
                }
            }
            getEventSinkRepository().stopAll()
        }
    }

}

class KafkaEventConsumer(private val consumer: KafkaConsumer<String, ByteArray>,
                         private val config: StreamsSinkConfiguration,
                         private val log: Log): StreamsEventConsumer<KafkaConsumer<String, ByteArray>>(consumer, config, log) {

    private var status: ConsumerStatus

    init {
        status = ConsumerStatus.INITIALIZED
    }

    override fun status(): ConsumerStatus {
        return status
    }

    private lateinit var topics: Set<String>

    override fun withTopics(topics: Set<String>): StreamsEventConsumer<KafkaConsumer<String, ByteArray>> {
        this.topics = topics
        return this
    }

    override fun start() {
        if (topics.isEmpty()) {
            log.info("No topics specified Kafka Consumer will not started")
            return
        }
        this.consumer.subscribe(topics)
        status = ConsumerStatus.RUNNING
    }

    override fun stop() {
        if (status == ConsumerStatus.STOPPED) return
        StreamsUtils.ignoreExceptions({
            status = ConsumerStatus.STOPPED
            consumer.close()
        }, UninitializedPropertyAccessException::class.java)
    }

    override fun read(): Map<String, List<Any>>? {
        if (status != ConsumerStatus.RUNNING) return null

        val start = System.currentTimeMillis()
        consumer.resume(consumer.assignment())
        val records = consumer.poll(Duration.ofMillis(config.sinkPollingInterval))
        consumer.pause(consumer.assignment())
        if (records != null && !records.isEmpty) {
            return records
                    .map {
                        it.topic()!! to JSONUtils.readValue<Any>(it.value())
                    }
                    .groupBy({ it.first }, { it.second })
        }
        return null
    }
}