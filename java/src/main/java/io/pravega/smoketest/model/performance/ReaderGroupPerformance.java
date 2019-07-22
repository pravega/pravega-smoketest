package io.pravega.smoketest.model.performance;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ReaderGroupPerformance extends PerformanceStats {
    private long eventsOutOfSequence;

    public long getEventsOutOfSequence() {
        return eventsOutOfSequence;
    }
    public void setEventsOutOfSequence(long eventsOutOfSequence) {
        this.eventsOutOfSequence = eventsOutOfSequence;
    }

    @JsonIgnore
    public void incrementEventsOutOfSequence() {
        eventsOutOfSequence++;
    }
}
