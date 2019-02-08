package streams

import org.apache.commons.lang3.StringUtils
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.internal.GraphDatabaseAPI
import streams.utils.Neo4jUtils
import java.util.concurrent.ConcurrentHashMap


private const val STREAMS_TOPIC_KEY: String = "streams.sink.topic"
private const val STREAMS_TOPIC_KEY_CYPHER: String = "$STREAMS_TOPIC_KEY.cypher."
private const val STREAMS_TOPIC_KEY_CDC: String = "$STREAMS_TOPIC_KEY.cdc"

class StreamsTopicService(private val db: GraphDatabaseAPI) {
    private val properties: GraphProperties = db.dependencyResolver.resolveDependency(EmbeddedProxySPI::class.java).newGraphPropertiesProxy()
    private val log = Neo4jUtils.getLogService(db).getUserLog(StreamsTopicService::class.java)

    fun clearAll() {
        val keys = properties.allProperties
                .filterKeys { it.startsWith(STREAMS_TOPIC_KEY) }
                .keys
        keys.forEach {
            properties.removeProperty(it)
        }
    }

    fun remove(topic: String) {
        if (topic.isNullOrBlank()) {
            return
        }
        val cypherTopic = "$STREAMS_TOPIC_KEY_CYPHER$topic"
        if (properties.hasProperty(cypherTopic)) {
            properties.removeProperty(cypherTopic)
        } else if (isCDCTopic(topic)) {
            val cdcTopics = properties
                    .getProperty(STREAMS_TOPIC_KEY_CDC)
                    .toString()
                    .split(";")
                    .toMutableSet()
            cdcTopics -= topic
            if (cdcTopics.isNotEmpty()) {
                properties.setProperty(STREAMS_TOPIC_KEY_CDC, cdcTopics.joinToString(";"))
            } else {
                properties.removeProperty(STREAMS_TOPIC_KEY_CDC)
            }
        } else if (log.isDebugEnabled) {
            log.debug("No query registered for topic $topic")
        }
    }

    fun setCypherTemplate(topic: String, query: String) {
        if (topic.isNullOrBlank() || query.isNullOrBlank()) {
            return
        }
        properties.setProperty("$STREAMS_TOPIC_KEY_CYPHER$topic", query)
    }

    fun setAllCypherTemplates(topics: Map<String, String>) {
        topics.forEach {
            setCypherTemplate(it.key, it.value)
        }
    }

    fun getCypherTemplate(topic: String): String? {
        val key = "$STREAMS_TOPIC_KEY_CYPHER$topic"
        if (!properties.hasProperty(key)) {
            if (log.isDebugEnabled) {
                log.debug("No query registered for topic $topic")
            }
            return null
        }
        return properties.getProperty(key).toString()
    }

    fun getAllCypherTemplates(): Map<String, String> {
        return properties.allProperties
                .filterKeys { it.startsWith(STREAMS_TOPIC_KEY_CYPHER) }
                .map { it.key.replace(STREAMS_TOPIC_KEY_CYPHER, StringUtils.EMPTY) to it.value.toString()}
                .toMap()
    }

    fun setAllCDCTopics(topics: Set<String>) {
        if (topics.isEmpty()) {
            return
        }
        val topicStr = topics.joinToString(";")
        properties.setProperty(STREAMS_TOPIC_KEY_CDC, topicStr)
    }

    fun setCDCTopic(topic: String) {
        if (topic.isNullOrBlank()) {
            return
        }
        if (properties.hasProperty(STREAMS_TOPIC_KEY_CDC)) {
            val cdcTopics = properties
                    .getProperty(STREAMS_TOPIC_KEY_CDC)
                    .toString()
                    .split(";")
                    .toMutableSet()
            cdcTopics += topic
            properties.setProperty(STREAMS_TOPIC_KEY_CDC, cdcTopics.joinToString(";"))
        } else {
            properties.setProperty(STREAMS_TOPIC_KEY_CDC, topic)
        }
    }

    fun getAllCDCTopics(): Set<String> {
        if (!properties.hasProperty(STREAMS_TOPIC_KEY_CDC)) {
            return emptySet()
        }
        return properties.getProperty(STREAMS_TOPIC_KEY_CDC, StringUtils.EMPTY)
                .toString()
                .split(";")
                .toSet()
    }

    fun getTopics(): Set<String> {
        return getAllCypherTemplates().keys + getAllCDCTopics()
    }

    fun isCDCTopic(topic: String): Boolean {
        return getAllCDCTopics().contains(topic)
    }

    fun exists(topic: String): Boolean {
        return getTopics().contains(topic)
    }

}