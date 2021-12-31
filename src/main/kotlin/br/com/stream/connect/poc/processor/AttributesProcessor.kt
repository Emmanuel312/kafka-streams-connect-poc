package br.com.stream.connect.poc.processor

import br.com.stream.connect.poc.entity.Attributes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration
import java.time.Instant

class AttributesProcessor : Processor<String, Attributes, String, Attributes> {

    private lateinit var kvStore: KeyValueStore<String, List<Attributes>>
    private lateinit var context: ProcessorContext<String, Attributes>

    override fun init(context: ProcessorContext<String, Attributes>) {
        this.context = context
        this.context.schedule(Duration.ofSeconds(20), PunctuationType.STREAM_TIME, this::punctuation)
        kvStore = context.getStateStore("attributes-store")
    }

    override fun process(record: Record<String, Attributes>) {
        println("process")
        val key = record.key()
        val value = record.value()

        val userAttributesList = kvStore[key] ?: emptyList()

        kvStore.put(key, userAttributesList.plus(value))
    }

    private fun punctuation(timestamp: Long) {
        println("Punctuation exec... with timestamp ${Instant.ofEpochMilli(timestamp)}")

        kvStore.all().use { iter ->
            val attributesList = mutableListOf<KeyValue<String, List<Attributes>>>()
            while (iter.hasNext()) {
                attributesList.add(iter.next())
            }
            val attributesListSorted = attributesList
                .flatMap { it.value }
                .sorted()

            attributesListSorted.forEach {
                context.forward(
                    Record(
                        it.userId,
                        it,
                        Instant.now().toEpochMilli()
                    )
                )
                kvStore.delete(it.userId)
            }
        }
    }
}