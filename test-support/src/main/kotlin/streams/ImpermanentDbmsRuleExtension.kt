package streams

import org.hamcrest.Matcher
import org.hamcrest.StringDescription
import org.neo4j.function.ThrowingSupplier
import org.neo4j.test.rule.DbmsRule
import streams.config.StreamsConfig
import java.util.concurrent.TimeUnit
import java.util.function.Function


fun DbmsRule.setConfig(key: String, value: String): DbmsRule {
    StreamsConfig.registerListener { it.put(key, value) }
    return this
}

fun DbmsRule.start(timeout: Long = 5000): DbmsRule {
    try {
        this.restartDatabase()
    } catch (e: NullPointerException) {
        val before = DbmsRule::class.java.getDeclaredMethod("before")
        before.isAccessible = true
        before.invoke(this)
    }
    if (!this.isAvailable(timeout)) {
        throw RuntimeException("Neo4j Instance Not Available")
    }
    return this
}

fun DbmsRule.shutdownSilently(): DbmsRule {
    try { this.shutdown() } catch (ignored: Exception) {}
    return this
}
