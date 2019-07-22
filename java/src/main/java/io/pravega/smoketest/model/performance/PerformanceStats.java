package io.pravega.smoketest.model.performance;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PerformanceStats {
    private long events;
    private double eventsPerSec;
    private double eventsPerWorkerPerSec;
    private long bytes;
    private float kbPerSec;
    private WorkerStats workerStats = new WorkerStats();

    public WorkerStats getWorkerStats() {
        return workerStats;
    }

    public void setWorkerStats(WorkerStats workerStats) {
        this.workerStats = workerStats;
    }

    public long getEvents() {
        return events;
    }

    public void setEvents(long events) {
        this.events = events;
    }

    public double getEventsPerSec() {
        return eventsPerSec;
    }

    public void setEventsPerSec(double eventsPerSec) {
        this.eventsPerSec = eventsPerSec;
    }

    public long getBytes() {
        return bytes;
    }

    public void setBytes(long bytes) {
        this.bytes = bytes;
    }

    public float getKbPerSec() {
        return kbPerSec;
    }

    public void setKbPerSec(float kbPerSec) {
        this.kbPerSec = kbPerSec;
    }

    public double getEventsPerWorkerPerSec() {
        return eventsPerWorkerPerSec;
    }

    public void setEventsPerWorkerPerSec(double eventsPerWorkerPerSec) {
        this.eventsPerWorkerPerSec = eventsPerWorkerPerSec;
    }
}
