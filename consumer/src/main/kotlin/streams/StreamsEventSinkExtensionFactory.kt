package streams

import org.neo4j.kernel.AvailabilityGuard
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.extension.KernelExtensionFactory
import org.neo4j.kernel.impl.logging.LogService
import org.neo4j.kernel.impl.spi.KernelContext
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.kernel.lifecycle.LifecycleAdapter
import streams.procedures.StreamsSinkProcedures
import streams.utils.Neo4jUtils
import streams.utils.Neo4jUtils.isWriteableInstance
import streams.utils.Neo4jUtils.readInTxWithGraphDatabaseService
import streams.utils.Neo4jUtils.writeInTxWithGraphDatabaseService
import streams.utils.StreamsUtils

class StreamsEventSinkExtensionFactory : KernelExtensionFactory<StreamsEventSinkExtensionFactory.Dependencies>("Streams.Consumer") {

    override fun newInstance(context: KernelContext, dependencies: Dependencies): Lifecycle {
        return StreamsEventLifecycle(dependencies)
    }

    interface Dependencies {
        fun graphdatabaseAPI(): GraphDatabaseAPI
        fun log(): LogService
        fun config(): Config
        fun availabilityGuard(): AvailabilityGuard
    }

    class StreamsEventLifecycle(private val dependencies: StreamsEventSinkExtensionFactory.Dependencies): LifecycleAdapter() {
        private val db = dependencies.graphdatabaseAPI()
        private val logService = dependencies.log()
        private val configuration = dependencies.config()
        private var streamsLog = logService.getUserLog(StreamsEventLifecycle::class.java)

        private lateinit var eventSink: StreamsEventSink

        override fun start() {
            try {
                dependencies.availabilityGuard().addListener(object: AvailabilityGuard.AvailabilityListener {
                    override fun unavailable() {}

                    override fun available() {
                        streamsLog.info("Initialising the Streams Sink module")
                        val streamsSinkConfiguration = StreamsSinkConfiguration.from(configuration)
                        val streamsTopicService = StreamsTopicService(db)
                        writeInTxWithGraphDatabaseService(db) {
                            streamsTopicService.clearAll()
                            streamsTopicService.setAllCypherTemplates(streamsSinkConfiguration.cypherTopics)
                            streamsTopicService.setAllCDCTopics(streamsSinkConfiguration.cdcTopics)
                            it.success()
                        }
                        val streamsQueryExecution = StreamsEventSinkQueryExecution(streamsTopicService, db, logService.getUserLog(StreamsEventSinkQueryExecution::class.java))

                        val log = logService.getUserLog(StreamsEventSinkFactory::class.java)
                        // Create and start the Sink
                        eventSink = StreamsEventSinkFactory
                                .getStreamsEventSink(configuration,
                                        streamsQueryExecution,
                                        streamsTopicService,
                                        log)

                        val allTopics = streamsSinkConfiguration.cypherTopics.keys + streamsSinkConfiguration.cdcTopics
                        allTopics.forEach {
                            eventSink.getEventSinkRepository().add(it, configuration.raw, log)
                        }

                        eventSink.start()

                        if (isWriteableInstance(db)) {
                            readInTxWithGraphDatabaseService(db) {
                                if (streamsLog.isDebugEnabled) {
                                    streamsLog.debug("Subscribed topics with Cypher queries: ${streamsTopicService.getAllCypherTemplates()}")
                                    streamsLog.debug("Subscribed topics with CDC configuration: ${streamsTopicService.getAllCDCTopics()}")
                                } else {
                                    streamsLog.info("Subscribed topics: ${streamsTopicService.getTopics()}")
                                }
                            }
                        }

                        // Register required services for the Procedures
                        StreamsSinkProcedures.registerStreamsSinkConfiguration(streamsSinkConfiguration)
                        StreamsSinkProcedures.registerStreamsEventConsumerFactory(eventSink.getEventConsumerFactory())
                        StreamsSinkProcedures.registerStreamsEventSinkConfigMapper(eventSink.getEventSinkConfigMapper())
                        StreamsSinkProcedures.registerStreamsEventSinkRepository(eventSink.getEventSinkRepository())
                        StreamsSinkProcedures.registerStreamsTopicService(streamsTopicService)
                        streamsLog.info("Streams Sink module initialised")
                    }

                })
            } catch (e: Exception) {
                e.properties.graphDatabase.beginTx()StackTrace()
                streamsLog.error("Error initializing the streaming sink", e)
            }
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

