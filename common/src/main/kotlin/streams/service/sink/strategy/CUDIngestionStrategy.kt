package streams.service.sink.strategy

import org.neo4j.graph_integration.Entity
import streams.service.StreamsSinkEntity


class CUDIngestionStrategy: IngestionStrategy {

    val strategy = org.neo4j.graph_integration.strategy.cud.CUDIngestionStrategy<Any, Any>()

    override fun mergeNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return strategy.mergeNodeEvents(events.map { Entity(it.key, it.value) })
            .events
            .map { QueryEvents(it.query, it.events) }
    }

    override fun deleteNodeEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    override fun mergeRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

    override fun deleteRelationshipEvents(events: Collection<StreamsSinkEntity>): List<QueryEvents> {
        return emptyList()
    }

}