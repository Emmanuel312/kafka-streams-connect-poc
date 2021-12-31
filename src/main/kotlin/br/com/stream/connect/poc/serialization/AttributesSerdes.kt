package br.com.stream.connect.poc.serialization

import br.com.stream.connect.poc.entity.Attributes
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer

class AttributesSerdes {
    companion object {
        private val json = ObjectMapper().registerKotlinModule()
        fun getSerdes(): Serde<Attributes> {
            return Serdes.serdeFrom(serializer, deserializer)
        }
    }

    object serializer : Serializer<Attributes> {
        override fun serialize(topic: String?, data: Attributes?): ByteArray = json.writeValueAsBytes(data)
    }

    object deserializer : Deserializer<Attributes> {
        override fun deserialize(topic: String?, data: ByteArray?): Attributes =
            json.readValue(data, Attributes::class.java)
    }
}