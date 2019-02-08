package streams

import org.neo4j.logging.Log
import java.util.concurrent.ConcurrentHashMap

class StreamsEventConsumerRepository(private val streamsEventConsumerFactory: StreamsEventConsumerFactory) {
    private val concurrentHashMap = ConcurrentHashMap<String, StreamsEventConsumer<*>>()

    fun add(topic: String, config: Map<String, String>, log: Log) {
        concurrentHashMap[topic] = streamsEventConsumerFactory
                .createStreamsEventConsumer(config, log)
                .withTopics(setOf(topic))
        start(topic)
    }

    fun start(topic: String) {
        concurrentHashMap[topic]?.start()
    }

    fun stop(topic: String) {
        concurrentHashMap[topic]?.stop()
    }

    fun stopAll() {
        concurrentHashMap.forEach { topic, consumer -> consumer.stop() }
    }

    fun remove(topic: String) {
        stop(topic)
        concurrentHashMap.remove(topic)
    }

    fun get(topic: String): StreamsEventConsumer<*>? {
        return concurrentHashMap[topic]
    }

    fun status(): List<Map<String, String>> {
        return concurrentHashMap.map {
            linkedMapOf("topic" to it.key,
                    "status" to it.value.status().toString())
        }
    }

    fun status(topic: String): ConsumerStatus? {
        return concurrentHashMap[topic]?.status()
    }

    fun topics(): Set<String> {
        return concurrentHashMap.keys
    }

    fun forEach(action: (String, StreamsEventConsumer<*>) -> Unit) {
        concurrentHashMap
                .forEach { topic: String, consumer: StreamsEventConsumer<*> -> action(topic, consumer) }
    }

    fun exists(topic: String): Boolean {
        return concurrentHashMap.containsKey(topic)
    }

}