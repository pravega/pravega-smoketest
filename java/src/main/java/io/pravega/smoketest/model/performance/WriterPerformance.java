package io.pravega.smoketest.model.performance;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;

public class WriterPerformance extends PerformanceStats {
    private long txStarted;
    private long txCommitted;
    private long txAborted;
    private long txFailed;
    private double txPerSec;
    private double blockedMeanTime = 0;
    private long blockedObservations = 0;

    public long getTxStarted() {
        return txStarted;
    }

    public void setTxStarted(long txStarted) {
        this.txStarted = txStarted;
    }

    public long getTxAborted() {
        return txAborted;
    }

    public void setTxAborted(long txAborted) {
        this.txAborted = txAborted;
    }

    public long getTxCommitted() {
        return txCommitted;
    }

    public void setTxCommitted(long txCommitted) {
        this.txCommitted = txCommitted;
    }

    public long getTxFailed() {
        return txFailed;
    }

    public void setTxFailed(long txFailed) {
        this.txFailed = txFailed;
    }

    public double getTxPerSec() {
        return txPerSec;
    }

    public void setTxPerSec(double txPerSec) {
        this.txPerSec = txPerSec;
    }

    public long getTimesBlocked() {
        return blockedObservations;
    }

    public void setTimesBlocked(int times) {
        this.blockedObservations = times;
    }

    public double getAverageBlockTime() {
        return blockedMeanTime;
    }

    public void setAverageBlockTime(double average) {
        this.blockedMeanTime = average;
    }

    @JsonIgnore
    public void addBlockedFor(long nanosBlockedFor) {
        blockedObservations++;
        blockedMeanTime += (nanosBlockedFor - blockedMeanTime) / blockedObservations;
    }

    public static void mergeBlockedStats(WriterPerformance destination, WriterPerformance...sources) {
        if (sources == null) {
            return;
        }
        long totalN = Arrays.stream(sources)
                        .map(WriterPerformance::getTimesBlocked)
                        .reduce(0l, Long::sum);
        double weightedMean = Arrays.stream(sources)
                                .map( (perf) -> perf.blockedMeanTime * perf.blockedObservations / totalN)
                                .reduce(0d, Double::sum);
        destination.blockedMeanTime = weightedMean;
        destination.blockedObservations = totalN;
    }
}
