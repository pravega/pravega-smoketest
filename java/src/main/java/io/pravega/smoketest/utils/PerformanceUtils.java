package io.pravega.smoketest.utils;

import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.performance.PerformanceStats;
import io.pravega.smoketest.model.performance.StreamPerformance;
import io.pravega.smoketest.model.performance.TestRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PerformanceUtils {
    private static Logger LOG = LoggerFactory.getLogger(PerformanceUtils.class);
    private static final long BYTES_IN_KB = 1024;
    private static DecimalFormat decimalFormat = new DecimalFormat("#,###");

    public static void updatePerSecondValues(StreamPerformance streamPerformance, long secondsRunning) {
        if (streamPerformance.getWriters() != null) {
            updatePerSecondValues(streamPerformance.getWriters(), secondsRunning);

            // Update TX stats
            streamPerformance.getWriters().setTxPerSec(perSecond(streamPerformance.getWriters().getTxCommitted(), secondsRunning));
        }

        if (streamPerformance.getReaders() != null) {
            updatePerSecondValues(streamPerformance.getReaders(), secondsRunning);

            // Update all the readerGroup stats
            streamPerformance.getReaderGroups().values()
                .forEach(p -> updatePerSecondValues(p, secondsRunning));
        }
    }

    public static void updatePerSecondValues(PerformanceStats performanceStats, long secondsRunning) {
        performanceStats.setEventsPerSec(perSecond(performanceStats.getEvents(), secondsRunning));
        performanceStats.setKbPerSec(kbPerSec(performanceStats.getBytes(), secondsRunning));

        if (performanceStats.getWorkerStats().getCount() > 0) {
            performanceStats.setEventsPerWorkerPerSec(perSecond(performanceStats.getEvents() / performanceStats.getWorkerStats().getCount(), secondsRunning));
        }
    }

    public static void dumpRuntime(String title, TestRuntime testRuntime) {
        long runningSeconds = testRuntime.getMinutesRunning() * 60;

        LOG.info("{}  Running for {}", title, testRuntime.getHumanRunning());

        for (Map.Entry<String, StreamPerformance> streamPerformance : testRuntime.getStreams().entrySet()) {
            LOG.info("Dumping stats for stream {}", streamPerformance.getKey());
            dumpStreamPerformance(streamPerformance.getValue(), runningSeconds);
        }
    }

    public static void dumpPayload(String title, PerformancePayload performancePayload) {
        long runningMillis = runningMillis(performancePayload.getStartTime(), performancePayload.getCurrentTime());
        long runningSeconds = TimeUnit.MILLISECONDS.toSeconds(runningMillis);

        StreamPerformance streamPerformance = performancePayload.getStreamPerformance();

        LOG.info("{}  Running for {}", title, toHumanDuration(runningMillis));
        dumpStreamPerformance(streamPerformance, runningSeconds);
    }

    /**
     * Formats the Duration in human readable form, i.e. 1h 4m
     */
    public static String toHumanDuration(long millis) {
        long day = (millis / (1000 * 60 * 60 * 24));
        long hour = (millis / (1000 * 60 * 60)) % 24;
        long minute = (millis / (1000 * 60)) % 60;
        long second = (millis / 1000) % 60;

        if (day > 0) {
            return String.format("%dd %dh", day, hour);
        }
        else if (hour > 0) {
            return String.format("%dh %dm", hour, minute);
        }
        else {
            return String.format("%dm %ds", minute, second);
        }
    }

    private static void dumpStreamPerformance(StreamPerformance streamPerformance, long runningSeconds) {
        if (streamPerformance.getWriters() != null) {
            LOG.info("Writers ({}/{}): events:{}, events/sec:{}, KB/sec:{}",
                streamPerformance.getWriters().getWorkerStats().getActive(),
                streamPerformance.getWriters().getWorkerStats().getCount(),
                toHumanNumber(streamPerformance.getWriters().getEvents()),
                toHumanNumber(perSecond(streamPerformance.getWriters().getEvents(), runningSeconds)),
                kbPerSec(streamPerformance.getWriters().getBytes(), runningSeconds));

            LOG.info(" Transactions: started: {}, perSec:{}, committed:{}, failed:{}, aborted:{}, aborted but read: {}",
                streamPerformance.getWriters().getTxStarted(),
                streamPerformance.getWriters().getTxPerSec(),
                streamPerformance.getWriters().getTxCommitted(),
                streamPerformance.getWriters().getTxFailed(),
                streamPerformance.getWriters().getTxAborted(),
                streamPerformance.getReaders().getTxAbortedRead());

            LOG.info(" Blocked Waiting For ACK: times blocks: {}, average block time (ms): {}",
                toHumanNumber(streamPerformance.getWriters().getTimesBlocked()),
                toHumanNumber(streamPerformance.getWriters().getAverageBlockTime()));
        }


        if (streamPerformance.getReaders() != null) {
            LOG.info("Readers ({}/{}): events:{}, events/sec:{}, KB/sec:{}",
                streamPerformance.getReaders().getWorkerStats().getActive(),
                streamPerformance.getReaders().getWorkerStats().getCount(),
                toHumanNumber(streamPerformance.getReaders().getEvents()),
                toHumanNumber(perSecond(streamPerformance.getReaders().getEvents(), runningSeconds)),
                kbPerSec(streamPerformance.getReaders().getBytes(), runningSeconds));
        }
    }
    private static float kbPerSec(long bytes, long seconds) {
        return seconds > 0 ? toKB(bytes / seconds) : Float.NaN;
    }

    private static double perSecond(long records, long seconds) {
        return seconds > 0 ? records * 1.0d /seconds : 0;
    }

    private static float toKB(long bytes) {
        return bytes * 1.0f / BYTES_IN_KB;
    }

    private static String toHumanNumber(double number) {
        return toHumanNumber((long)number);
    }

    private static String toHumanNumber(long number) {
        return decimalFormat.format(number);
    }

    private static long runningMillis(long startTimeMillis, long currentTimeMillis) {
        if (currentTimeMillis > startTimeMillis) {
            return currentTimeMillis - startTimeMillis;
        }

        return 0;
    }
}
