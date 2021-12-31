package br.com.stream.connect.poc

import br.com.stream.connect.poc.processor.AttributesProcessor
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import java.time.Duration
import java.util.*


class KafkaStreamsServer(private val json: ObjectMapper) {
    fun start() {
        val properties = loadProperties()
        val attributesTopology = createTopology()
        val kafkaStreams = KafkaStreams(attributesTopology, properties)

        kafkaStreams.start()
    }

    private fun createTopology(): Topology {
        val sourceTopic = "user-attributes-source"
        val sinkTopic = "user-attributes-sink"
        val stringSerdes = Serdes.String()
        val storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("attributes-store"),
            stringSerdes,
            stringSerdes
        )
        val topology = Topology()

        topology
            .addSource("source-topic", stringSerdes.deserializer(), stringSerdes.deserializer(), sourceTopic)
            .addProcessor("attributes-processor", ProcessorSupplier { AttributesProcessor(json) }, "source-topic")
            .addStateStore(storeBuilder, "attributes-processor")
            .addSink(
                "sink-topic",
                sinkTopic,
                stringSerdes.serializer(),
                stringSerdes.serializer(),
                "attributes-processor"
            )

        return topology
    }

    private fun loadProperties(): Properties {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "kafka-streams-connect-poc"
        streamsConfiguration[StreamsConfig.CLIENT_ID_CONFIG] = "kafka-streams-connect-poc"
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
//        streamsConfiguration[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryUrl
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
//        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = GenericAvroSerde::class.java
//        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
//        streamsConfiguration[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 10 * 1000
        streamsConfiguration[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] = AttributesTimestampExtractor::class.java

        return streamsConfiguration
    }
}