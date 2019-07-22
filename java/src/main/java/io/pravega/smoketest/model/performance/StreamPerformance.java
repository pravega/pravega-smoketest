package io.pravega.smoketest.model.performance;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public class StreamPerformance {
    private WriterPerformance writers;
    private ReaderPerformance readers;

    public WriterPerformance getWriters() {
        return writers;
    }

    public void setWriters(WriterPerformance writers) {
        this.writers = writers;
    }

    public ReaderPerformance getReaders() {
        return readers;
    }

    public void setReaders(ReaderPerformance readers) {
        this.readers = readers;
    }

    @JsonIgnore
    public Map<String, ReaderGroupPerformance> getReaderGroups() {
        return readers.getReaderGroups();
    }

    @JsonIgnore
    public ReaderGroupPerformance getReaderGroup(String id) {
        return readers.getReaderGroups().get(id);
    }
}
