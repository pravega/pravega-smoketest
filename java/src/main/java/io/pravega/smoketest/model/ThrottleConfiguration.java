package io.pravega.smoketest.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Configuration option for Throttling Readers and Writers
 */
public class ThrottleConfiguration {
    private int maxEventsPerSecond = -1;
    private int maxOutstandingAcks = -1;
    private int dynamicThrottlePeriod = -1;

    @JsonIgnore
    private Clock startTime = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    @JsonIgnore
    public int getCurrentMaxEventsPerSec() {
        return getCurrentMaxEventsPerSec(Instant.now());
    }

    @JsonIgnore
    public int getCurrentMaxEventsPerSec(Instant atTime) {
        if (!isEventsPerSecActive() || !isDynamicRate()) {
            return getMaxEventsPerSecond();
        }

        long timeSinceStart = startTime.instant().until(atTime, SECONDS);
        long placeInPeriod = timeSinceStart % (getDynamicThrottlePeriod() * 60);
        double percentDone = placeInPeriod / (getDynamicThrottlePeriod() * 60.0);

        // value from 0.1 to 1, sine wave
        //1  /\
        //0.1    \/
        double scaleBy = 0.55 + 0.45 * Math.sin(percentDone * 2 * Math.PI);

        return (int) Math.round(scaleBy * getMaxEventsPerSecond());
    }

    public int getMaxEventsPerSecond() {
        return this.maxEventsPerSecond;
    }

    public void setMaxEventsPerSecond(int maxEventsPerSecond) {
        this.maxEventsPerSecond = maxEventsPerSecond;
    }

    public int getMaxOutstandingAcks() {
        return maxOutstandingAcks;
    }

    public void setMaxOutstandingAcks(int maxOutstandingAcks) {
        this.maxOutstandingAcks = maxOutstandingAcks;
    }

    public int getDynamicThrottlePeriod() {
        return dynamicThrottlePeriod;
    }

    public void setDynamicThrottlePeriod(int dynamicThrottlePeriod) {
        this.dynamicThrottlePeriod = dynamicThrottlePeriod;
    }

    @JsonIgnore
    public boolean isThrottlePerSecond() {
        return isEventsPerSecActive();
    }

    @JsonIgnore
    public boolean isEventsPerSecActive() {
        return maxEventsPerSecond > 0;
    }

    @JsonIgnore
    public boolean isDynamicRate() {
        return dynamicThrottlePeriod > 0;
    }

    @JsonIgnore
    public boolean isThrottleAcks() {
        return maxOutstandingAcks > 0;
    }

    @JsonIgnore
    public boolean isWaitForAck() {
        return maxOutstandingAcks == 1;
    }

    @JsonIgnore
    public void setWaitForAck(boolean waitForAck) {
        this.maxOutstandingAcks = waitForAck ? 1 : -1;
    }

    @JsonIgnore
    public void setNewStartTime(Instant newStartTime) {
        this.startTime = Clock.fixed(newStartTime, ZoneId.systemDefault());
    }
}
