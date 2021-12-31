package br.com.stream.connect.poc

import br.com.stream.connect.poc.entity.Attributes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.Instant

class AttributesTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
        val attributes = record.value() as Attributes

        return Instant.parse(attributes.eventDateTime).toEpochMilli()
    }
}