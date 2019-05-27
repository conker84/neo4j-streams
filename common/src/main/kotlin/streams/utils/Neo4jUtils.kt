package streams.utils

import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.internal.LogService
import java.lang.reflect.InvocationTargetException
import java.util.stream.Collectors

object Neo4jUtils {
    fun isWriteableInstance(db: GraphDatabaseAPI): Boolean {
        try {
            val isSlave = StreamsUtils.ignoreExceptions(
                    {
                        val hadb = Class.forName("org.neo4j.kernel.ha.HighlyAvailableGraphDatabase")
                        hadb.isInstance(db) && !(hadb.getMethod("isMaster").invoke(db) as Boolean)
                    }, ClassNotFoundException::class.java, IllegalAccessException::class.java,
                    InvocationTargetException::class.java, NoSuchMethodException::class.java)
            if (isSlave != null && isSlave) {
                return false
            }

            val role = db.execute("CALL dbms.cluster.role()").columnAs<String>("role").next()
            println("Role is: $role")
            return role.equals("LEADER", ignoreCase = true)
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return true
            }
            throw e
        }
    }

    fun isCluster(db: GraphDatabaseAPI): Boolean {
        try {
            db.execute("CALL dbms.cluster.role()").columnAs<String>("role").next()
            return true
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    fun clusterHasLeader(db: GraphDatabaseAPI): Boolean {
        try {
            val overview = db.execute("""
                call dbms.cluster.overview() YIELD role
                return role
            """.trimIndent())
                    .stream()
                    .map { it["role"].toString() }
                    .collect(Collectors.toList())
            println("Cluster Overview $overview")
            return overview.contains("LEADER")
        } catch (e: QueryExecutionException) {
            if (e.statusCode.equals("Neo.ClientError.Procedure.ProcedureNotFound", ignoreCase = true)) {
                return false
            }
            throw e
        }
    }

    fun getLogService(db: GraphDatabaseAPI): LogService {
        return db.dependencyResolver
                .resolveDependency(LogService::class.java)
    }
}