package br.com.stream.connect.poc

import br.com.stream.connect.poc.processor.AttributesProcessor
import br.com.stream.connect.poc.serialization.AttributesListSerdes
import br.com.stream.connect.poc.serialization.AttributesSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.state.Stores
import java.util.*


class KafkaStreamsServer {
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
            AttributesListSerdes.getSerdes()
        )
        val topology = Topology()

        topology

            .addSource(
                "source-topic",
                stringSerdes.deserializer(),
                AttributesSerdes.deserializer,
                sourceTopic
            )
            .addProcessor("attributes-processor", ProcessorSupplier { AttributesProcessor() }, "source-topic")
            .addStateStore(storeBuilder, "attributes-processor")
            .addSink(
                "sink-topic",
                sinkTopic,
                stringSerdes.serializer(),
                AttributesSerdes.serializer,
                "attributes-processor"
            )

        return topology
    }

    private fun loadProperties(): Properties {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "kafka-streams-connect-poc"
        streamsConfiguration[StreamsConfig.CLIENT_ID_CONFIG] = "kafka-streams-connect-poc"
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
//        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
//        streamsConfiguration[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 10 * 1000
        streamsConfiguration[StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG] =
            AttributesTimestampExtractor::class.java

        return streamsConfiguration
    }
}