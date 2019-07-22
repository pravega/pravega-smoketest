package io.pravega.smoketest.model

import io.pravega.smoketest.testworker.workers.events.EventGenerator
import io.pravega.smoketest.testworker.workers.events.GeneratedEvent
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.text.RandomStringGenerator
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit.SECONDS
import java.util.Random
import java.util.UUID
import kotlin.streams.toList

private val rgen = Random()
private val generator = RandomStringGenerator.Builder().build()

class PayloadConfiguration
    : EventGenerator {

    val minimumSize: Int
    val maximumSize: Int
    var numberOfKeys: Int
        set(value) {
            if (this.keys.size != value) {
                this.keys = java.util.stream.Stream.generate { UUID.randomUUID().toString() }
                    .limit(value.toLong())
                    .toList()
            }
            field = value
        }

    val dynamicKeyProbability : Boolean

    @JsonCreator
    constructor(@JsonProperty("minimumSize") minimumSize: Int = 100,@JsonProperty("maximumSize") maximumSize: Int = minimumSize,
                @JsonProperty("numberOfKeys") numberOfKeys: Int = 100, @JsonProperty("dynamicKeyProbability") dynamicKeyProbability : Boolean = false) {
        this.minimumSize = minimumSize
        this.maximumSize = maximumSize
        this.numberOfKeys = numberOfKeys
        this.dynamicKeyProbability = dynamicKeyProbability
    }

    constructor(size: Int) : this(size, size)

    @JsonIgnore
    var startTime : Clock = Clock.fixed(Instant.now(), ZoneId.systemDefault())
    var keys: List<String> = listOf()
    @JsonIgnore
    val hotkeyMovementMins = 10
    @JsonIgnore
    private val LOG = LoggerFactory.getLogger(PayloadConfiguration::class.java)

    @JsonIgnore
    fun getNormalRandom(): Double {
        // Clip into [-2, 2]
        var randomSample: Double
        do {
            randomSample = rgen.nextGaussian()
        } while (Math.abs(randomSample) > 2.0 )

        //convert it to [0, 1]
        return (randomSample + 2.0) / 4.0
    }

    @JsonIgnore
    fun getRoutingKey(): String {
        return getRoutingKey(Instant.now())
    }

    @JsonIgnore
    fun getRoutingKey(timeNow : Instant) : String {
        if (!dynamicKeyProbability) {
            return keys[rgen.nextInt(keys.size)]
        }

        // get number between [0,1], transform to int in [0, numberOfKeys)
        // Since this is a normal random number, the values in the middle are 'hotter' (more likely to be picked)
        val normalRandom = getNormalRandom()
        val normalRoutingKey = Math.floor(numberOfKeys * normalRandom).toInt()

        // every hotkeyMovementMins, the 'hottest' key moves over by one w/ wrap around
        val skew = startTime.instant().until(timeNow, SECONDS).toInt() / (60 * hotkeyMovementMins)
        val skewedRoutingKey = (normalRoutingKey + skew) % numberOfKeys

        try {
            return keys[skewedRoutingKey]
        } catch (e : Exception) {
            LOG.error("Numbers: ${normalRandom} ${normalRoutingKey} ${skew} ${skewedRoutingKey} ${keys.size} ${numberOfKeys}")
            throw e
        }
    }

    @JsonIgnore
    fun payloadSample(): String {
        return if (maximumSize > minimumSize) {
            generator.generate(rgen.nextInt(maximumSize - minimumSize) + minimumSize)
        } else {
            generator.generate(minimumSize)
        }
    }

    @JsonIgnore
    override fun nextEvent(): GeneratedEvent {
        return GeneratedEvent(getRoutingKey(), payloadSample())
    }
}
