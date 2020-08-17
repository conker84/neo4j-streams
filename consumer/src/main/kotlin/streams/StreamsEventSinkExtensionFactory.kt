package streams

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.availability.AvailabilityListener
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import org.neo4j.logging.internal.LogService
import streams.config.StreamsConfig
import streams.extensions.isSystemDb
import streams.procedures.StreamsSinkProcedures
import streams.service.TopicUtils
import streams.utils.Neo4jUtils
import streams.utils.StreamsUtils

class StreamsEventSinkExtensionFactory : ExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>(ExtensionType.DATABASE,"Streams.Consumer") {

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        return StreamsEventLifecycle(dependencies)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun dbms(): DatabaseManagementService
        fun log(): LogService
        fun streamsConfig(): StreamsConfig
        fun availabilityGuard(): AvailabilityGuard
    }

    class StreamsEventLifecycle(private val dependencies: Dependencies): LifecycleAdapter() {
        private val db = dependencies.graphdatabaseAPI()
        private val dbms = dependencies.dbms()
        private val logService = dependencies.log()
        private val configuration = dependencies.streamsConfig()
        private var streamsLog = logService.getUserLog(StreamsEventLifecycle::class.java)

        private lateinit var eventSink: StreamsEventSink

        override fun start() {
            if (db.isSystemDb()) {
                return
            }
            dependencies.availabilityGuard().addListener(object: AvailabilityListener {
                override fun unavailable() {}

                override fun available() {
                    try {
                        val systemDbWaitTimeout = configuration.getSystemDbWaitTimeout()
                        val whenAvailable = {
                            configuration.loadStreamsConfiguration()
                            if (!configuration.isSinkEnabled(db.databaseName())) {
                                Unit
                            } else {
                                streamsLog.info("Initialising the Streams Sink module")
                                val streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration, db.databaseName())
                                val streamsTopicService = StreamsTopicService()
                                val strategyMap = TopicUtils.toStrategyMap(streamsSinkConfiguration.topics,
                                        streamsSinkConfiguration.sourceIdStrategyConfig)
                                val streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db,
                                        logService.getUserLog(StreamsEventSinkQueryExecution::class.java),
                                        strategyMap)

                                // Create the Sink
                                val log = logService.getUserLog(StreamsEventSinkFactory::class.java)
                                eventSink = StreamsEventSinkFactory
                                        .getStreamsEventSink(configuration,
                                                streamsQueryExecution,
                                                streamsTopicService,
                                                log,
                                                db)

                                StreamsSinkProcedures.registerStreamsSinkConfiguration(streamsSinkConfiguration)
                                StreamsSinkProcedures.registerStreamsEventConsumerFactory(eventSink.getEventConsumerFactory())
                                StreamsSinkProcedures.registerStreamsEventSinkConfigMapper(eventSink.getEventSinkConfigMapper())
                                StreamsSinkProcedures.registerStreamsEventSink(eventSink)

                                // start the Sink
                                if (Neo4jUtils.isCluster(db)) {
                                    log.info("The Sink module is running in a cluster, checking for the ${Neo4jUtils.LEADER}")
                                    Neo4jUtils.waitForTheLeader(db, log) { initSinkModule(streamsTopicService, streamsSinkConfiguration) }
                                } else {
                                    // check if is writeable instance
                                    Neo4jUtils.executeInWriteableInstance(db) { initSinkModule(streamsTopicService, streamsSinkConfiguration) }
                                }
                                Unit
                            }
                        }
                        val whenNotAvailable = {
                            streamsLog.info("""
                                |Cannot start Streams Sink module because database ${Neo4jUtils.SYSTEM_DATABASE_NAME} 
                                |is not available after $systemDbWaitTimeout ms
                            """.trimMargin())
                        }
                        Neo4jUtils.executeWhenSystemDbIsAvailable(dbms,
                                configuration, whenAvailable, whenNotAvailable)
                    } catch (e: Exception) {
                        streamsLog.error("Error initializing the streaming sink:", e)
                    }
                }
            })
        }

        private fun initSinkModule(streamsTopicService: StreamsTopicService, streamsSinkConfiguration: StreamsSinkConfiguration) {
            if (streamsSinkConfiguration.checkApocTimeout > -1) {
                GlobalScope.launch(Dispatchers.IO) {
                    val success = StreamsUtils.blockUntilTrueOrTimeout(streamsSinkConfiguration.checkApocTimeout, streamsSinkConfiguration.checkApocInterval) {
                        val hasApoc = Neo4jUtils.hasApoc(db)
                        if (!hasApoc && streamsLog.isDebugEnabled) {
                            streamsLog.debug("APOC not loaded yet, next check in ${streamsSinkConfiguration.checkApocInterval} ms")
                        }
                        hasApoc
                    }
                    if (success) {
                        initSink(streamsTopicService, streamsSinkConfiguration)
                    } else {
                        streamsLog.info("Streams Sink plugin not loaded as APOC are not installed")
                    }
                }
            } else {
                initSink(streamsTopicService, streamsSinkConfiguration)
            }

        }

        private fun initSink(streamsTopicService: StreamsTopicService, streamsSinkConfiguration: StreamsSinkConfiguration) {
            streamsTopicService.clearAll()
            streamsTopicService.setAll(streamsSinkConfiguration.topics)
            eventSink.start()
            eventSink.printInvalidTopics()
        }

        override fun stop() {
            try {
                StreamsUtils.ignoreExceptions({ eventSink.stop() }, UninitializedPropertyAccessException::class.java)
            } catch (e : Throwable) {
                val message = e.message ?: "Generic error, please check the stack trace:"
                streamsLog.error(message, e)
            }
        }
    }
}

