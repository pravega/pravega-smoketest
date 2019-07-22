package io.pravega.smoketest.model.performance;

import io.pravega.smoketest.model.ReaderInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

public class ReaderPerformance extends PerformanceStats {
    private Map<String, ReaderGroupPerformance> readerGroups = new HashMap<>();
    private long eventsOutOfSequence;
    private Map<String, ReaderInfo> readers = new HashMap<>();

    public Map<String, ReaderGroupPerformance> getReaderGroups() {
        return readerGroups;
    }
    private long txAbortedRead;

    public void setReaderGroups(Map<String, ReaderGroupPerformance> readerGroups) {
        this.readerGroups = readerGroups;
    }

    public void addReaderGroup(String name, ReaderGroupPerformance performanceStats) {
        readerGroups.put(name, performanceStats);
    }

    public long getTxAbortedRead() {
        return txAbortedRead;
    }

    public void setTxAbortedRead(long txAbortedRead) {
        this.txAbortedRead = txAbortedRead;
    }

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

    public Map<String, ReaderInfo> getReaders() {
        return readers;
    }

    public void setReaders(Map<String, ReaderInfo> readers) {
        this.readers = readers;
    }
}
