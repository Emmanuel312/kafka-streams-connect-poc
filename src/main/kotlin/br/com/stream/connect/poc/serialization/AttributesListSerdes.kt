package br.com.stream.connect.poc.serialization

import br.com.stream.connect.poc.entity.Attributes
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer

class AttributesListSerdes {
    companion object {
        private val json = ObjectMapper().registerKotlinModule()
        fun getSerdes(): Serde<List<Attributes>> {
            return Serdes.serdeFrom(serializer, deserializer)
        }
    }

    object serializer : Serializer<List<Attributes>> {
        override fun serialize(topic: String?, data: List<Attributes>?): ByteArray = json.writeValueAsBytes(data)
    }

    object deserializer : Deserializer<List<Attributes>> {
        override fun deserialize(topic: String?, data: ByteArray?): List<Attributes> =
            json.readValue(data, object : TypeReference<List<Attributes>>() {})
    }
}