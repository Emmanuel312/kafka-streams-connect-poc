package br.com.stream.connect.poc.processor

import br.com.stream.connect.poc.entity.Attributes
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration
import java.time.Instant

class AttributesProcessor(private val json: ObjectMapper) : Processor<String, String, String, String> {

    private lateinit var kvStore: KeyValueStore<String, String>
    private lateinit var context: ProcessorContext<String, String>

    override fun init(context: ProcessorContext<String, String>) {
        this.context = context
        this.context.schedule(Duration.ofSeconds(20), PunctuationType.STREAM_TIME, this::punctuation)
        kvStore = context.getStateStore("attributes-store")
    }

    override fun process(record: Record<String, String>) {
        println("process")
        val key = record.key()
        val value = record.value()

        val userAttributes = kvStore[key]

        val userAttributesDeserialized = userAttributes?.let {
            json.readValue(userAttributes, object : TypeReference<List<Attributes>>() {})
        } ?: emptyList()

        val valueDeserialized = json.readValue(value, Attributes::class.java)
        val newUserAttributesDeserialized = userAttributesDeserialized.plus(valueDeserialized)
        kvStore.put(key, json.writeValueAsString(newUserAttributesDeserialized))
    }

    private fun punctuation(timestamp: Long) {
        println("Punctuation exec... with timestamp ${Instant.ofEpochMilli(timestamp)}")

        kvStore.all().use { iter ->
            val attributesList = mutableListOf<KeyValue<String, String>>()
            while (iter.hasNext()) {
                attributesList.add(iter.next())
            }
            val attributesListSorted =
                attributesList.flatMap { json.readValue(it.value, object : TypeReference<List<Attributes>>() {}) }
                    .sorted()

            attributesListSorted.forEach {
                context.forward(
                    Record(
                        it.userId,
                        json.writeValueAsString(it),
                        Instant.now().toEpochMilli()
                    )
                )
                kvStore.delete(it.userId)
            }

        }
    }
}