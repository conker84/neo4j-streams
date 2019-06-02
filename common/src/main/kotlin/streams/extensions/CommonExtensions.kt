package streams.extensions

import org.neo4j.graphdb.Node
import javax.lang.model.SourceVersion

fun Map<String,String>.getInt(name:String, defaultValue: Int) = this.get(name)?.toInt() ?: defaultValue

fun Node.labelNames() : List<String> {
    return this.labels.map { it.name() }
}

fun String.toPointCase(): String {
    return this.split("(?<=[a-z])(?=[A-Z])".toRegex()).joinToString(separator = ".").toLowerCase()
}

fun String.quote(): String = if (SourceVersion.isIdentifier(this)) this else "`$this`"

fun Map<String, Any?>.flatten(map: Map<String, Any?> = this, prefix: String = ""): Map<String, Any?> {
    return map.flatMap {
        val key = it.key
        val value = it.value
        val newKey = if (prefix != "") "$prefix.$key" else key
        if (value is Map<*, *>) {
            flatten(value as Map<String, Any>, newKey).toList()
        } else {
            listOf(newKey to value)
        }
    }.toMap()
}