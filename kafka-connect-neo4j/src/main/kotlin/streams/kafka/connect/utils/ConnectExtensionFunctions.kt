package streams.kafka.connect.utils

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.kafka.connect.sink.converters.MapValueConverter
import streams.serialization.JSONUtils
import streams.service.StreamsSinkEntity

private val log: Logger = LoggerFactory.getLogger(SinkRecord::class.java)

fun SinkRecord.toStreamsSinkEntity(): StreamsSinkEntity = StreamsSinkEntity(
        convertData("key", this.key()),
        convertData("value", this.value()))

private val converter = MapValueConverter<Any>()

private fun convertData(name: String, data: Any?) = try {
        when (data) {
            is Struct -> converter.convert(data)
            else -> if (data == null) null else JSONUtils.readValue<Any>(data)
        }
    } catch (e: Exception) {
        log.error("Error while converting SinkRecord field $name, with content $data, because of following exception", e)
        data
    }
