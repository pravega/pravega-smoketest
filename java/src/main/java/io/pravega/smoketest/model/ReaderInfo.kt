package io.pravega.smoketest.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Duration
import java.time.Instant

@JsonIgnoreProperties(value="lag", allowGetters=true)
data class ReaderInfo @JvmOverloads constructor(
    val readerID: String,
    val readerGroupName: String,
    val lastEventTime: Instant,
    val lastEventSize: Int,
    val forgetful: Boolean,
    val lastEventReadAt: Instant = Instant.now()) {
    val lag: Duration = Duration.between(lastEventTime, lastEventReadAt)
}

