package streams.service

import streams.events.*
import streams.serialization.JSONUtils
import streams.utils.StreamsUtils

abstract class StreamsSinkService {

    abstract fun isCDCTopic(topic: String): Boolean
    abstract fun getCypherTemplate(topic: String): String?
    abstract fun write(query: String, events: Collection<Any>)

    private fun writeWithCDCTemplate(params: Collection<Any>) {
        val separator = "`:`"
        val data = params.map { JSONUtils.asStreamsTransactionEvent(it) }
                .groupBy { it.payload.type }

        val nodes = data[EntityType.node].orEmpty()
        writeCDCNodes(nodes, separator)

        val rels = data[EntityType.relationship].orEmpty()
        writeCDCRelationships(rels, separator)
    }

    private fun writeCDCRelationships(rels: List<StreamsTransactionEvent>, separator: String) {
        rels.map {
                    val payload = it.payload as RelationshipPayload
                    val changeEvt = when (it.meta.operation) {
                        OperationType.deleted -> {
                            it.payload.before as RelationshipChange
                        }
                        else -> it.payload.after as RelationshipChange
                    }
                    val start = payload.start.labels?.joinToString(separator) ?: ""
                    val end = payload.end.labels?.joinToString(separator) ?: ""
                    mapOf("start" to start, "label" to payload.label, "end" to end) to mapOf("id" to payload.id,
                            "start" to payload.start.id, "end" to payload.end.id, "properties" to changeEvt.properties)
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .forEach { path, events ->
                    val query = """
                                ${StreamsUtils.UNWIND}
                                MERGE (start:`${path.getValue("start")}`{`streams_id`: event.start})
                                MERGE (end:`${path.getValue("end")}`{`streams_id`: event.end})
                                MERGE (start)-[r:`${path.getValue("label")}`{`streams_id`: event.id}]->(end)
                                    SET r += event.properties
                            """.trimIndent()
                    write(query, events)
                }
    }

    private fun writeCDCNodes(nodes: List<StreamsTransactionEvent>, separator: String) {
        nodes.map {
                    val changeEvt = when (it.meta.operation) {
                        OperationType.deleted -> {
                            it.payload.before as NodeChange
                        }
                        else -> it.payload.after as NodeChange
                    }
                    val labels = changeEvt.labels?.joinToString(separator) ?: ""
                    labels to mapOf("id" to it.payload.id, "properties" to changeEvt.properties)
                }
                .groupBy { it.first }
                .mapValues { it.value.map { it.second } }
                .forEach { labels, events ->
                    val query = "${StreamsUtils.UNWIND} MERGE(n:`$labels`{`streams_id`: event.id}) SET n += event.properties"
                    write(query, events)
                }
    }

    private fun writeWithCypherTemplate(topic: String, params: Collection<Any>) {
        val query = getCypherTemplate(topic) ?: return
        write(query, params)
    }

    fun writeForTopic(topic: String, params: Collection<Any>) {
        if (isCDCTopic(topic)) {
            writeWithCDCTemplate(params)
        } else {
            writeWithCypherTemplate(topic, params)
        }
    }

}