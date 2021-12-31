package br.com.stream.connect.poc

import br.com.stream.connect.poc.entity.Attributes
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.Instant

class AttributesTimestampExtractor(private val json : ObjectMapper = ObjectMapper().registerKotlinModule()) : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
        val attributes = json.readValue(record.value() as String, Attributes::class.java)

        return Instant.parse(attributes.eventDateTime).toEpochMilli()
    }
}