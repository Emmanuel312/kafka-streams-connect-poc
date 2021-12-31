package br.com.stream.connect.poc.entity

import java.time.Instant

data class Attributes(
    val userId: String,
    val properties: Map<String, Any>,
    val eventDateTime: String,
) : Comparable<Attributes> {
    override fun compareTo(other: Attributes): Int {
        val timestamp = Instant.parse(this.eventDateTime).toEpochMilli()
        val otherTimestamp = Instant.parse(other.eventDateTime).toEpochMilli()

        return timestamp.compareTo(otherTimestamp)
    }

}
