package br.com.stream.connect.poc

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

fun main() {
    val json = ObjectMapper().registerKotlinModule()
    val kafkaStreamsServer = KafkaStreamsServer(json)
    kafkaStreamsServer.start()

}