package io.pravega.smoketest.testworker.workers;

import io.pravega.smoketest.model.ReaderInfo;
import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.performance.PerformanceStats;
import io.pravega.smoketest.model.performance.ReaderGroupPerformance;
import io.pravega.smoketest.model.performance.ReaderPerformance;
import io.pravega.smoketest.model.performance.StreamPerformance;
import io.pravega.smoketest.model.performance.WriterPerformance;
import io.pravega.smoketest.testworker.MessageClient;
import io.pravega.smoketest.utils.PerformanceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskPerformanceCollector {
    private static final Logger LOG = LoggerFactory.getLogger(TaskPerformanceCollector.class);
    public static final int SENDING_SCHEDULE_SECS = 30;

    private MessageClient messageClient;

    @GuardedBy("streamPerformance")
    private final StreamPerformance streamPerformance = new StreamPerformance();

    private long startMillis;
    private String streamName;
    private ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    public TaskPerformanceCollector(MessageClient messageClient) {
        this.messageClient = messageClient;
    }

    public void start(String streamName) {
        this.streamName = streamName;
        startMillis = System.currentTimeMillis();

        LOG.info("Starting Performance Collector");
        scheduledExecutor.scheduleAtFixedRate(this::sendPerformance,
            SENDING_SCHEDULE_SECS,
            SENDING_SCHEDULE_SECS,
            TimeUnit.SECONDS);
    }

    public void stop() {
        LOG.info("Shutting Down Performance Collector");
        scheduledExecutor.shutdown();

        sendPerformance();
    }


    public void readerStarted(String readerGroup) {
        inLock(()->{
            if (streamPerformance.getReaders() == null) {
                streamPerformance.setReaders(new ReaderPerformance());
            }

            addCount(streamPerformance.getReaders(), 1);
            addActive(streamPerformance.getReaders(), 1);

            PerformanceStats readerGroupPerf = streamPerformance.getReaderGroups()
                .computeIfAbsent(readerGroup, (x) -> new ReaderGroupPerformance());
            addCount(readerGroupPerf, 1);
            addActive(readerGroupPerf, 1);
        });
    }

    public void readerEnded(String readerGroup) {
        inLock(() -> {
            addActive(streamPerformance.getReaders(), -1);
            addDead(streamPerformance.getReaders(), 1);

            PerformanceStats readerGroupPerf = streamPerformance.getReaderGroup(readerGroup);
            addActive(readerGroupPerf, -1);
            addDead(readerGroupPerf, 1);
        });
    }

    public void writerStarted() {
        inLock(() -> {
           if (streamPerformance.getWriters() == null) {
               streamPerformance.setWriters(new WriterPerformance());
           }

           addCount(streamPerformance.getWriters(), 1);
           addActive(streamPerformance.getWriters(), 1);
        });
    }


    public void blockedWaitingOnAck(long timeTook) {
        inLock(()->{
            streamPerformance.getWriters().addBlockedFor(timeTook);
        });
    }

    public void writerEnded() {
        inLock(() -> {
            addActive(streamPerformance.getWriters(), -1);
            addDead(streamPerformance.getWriters(), 1);
        });
    }

    public void eventWritten(long bytes) {
        inLock(() -> {
            addEvents(streamPerformance.getWriters(), 1);
            addBytes(streamPerformance.getWriters(), bytes);
        });
    }

    public void eventRead(ReaderInfo readerSnapshot) {
        inLock(() -> {
            addEvents(streamPerformance.getReaders(), 1);
            addBytes(streamPerformance.getReaders(), readerSnapshot.getLastEventSize());

            PerformanceStats readerGroupPerf = streamPerformance.getReaderGroup(readerSnapshot.getReaderGroupName());

            addEvents(readerGroupPerf, 1);
            addBytes(readerGroupPerf, readerSnapshot.getLastEventSize());
            updateLag(streamPerformance.getReaders(), readerSnapshot);
        });
    }

    public void updateLag(ReaderPerformance readers, ReaderInfo readerSnapshot) {
        readers.getReaders().put(readerSnapshot.getReaderID(), readerSnapshot);
    }

    public void eventOutOfSequence(String readerGroup) {
        inLock(() -> {
            streamPerformance.getReaders().incrementEventsOutOfSequence();
            streamPerformance.getReaderGroup(readerGroup).incrementEventsOutOfSequence();
        });
    }

    public void txStarted() {
        inLock(() -> {
            streamPerformance.getWriters().setTxStarted(streamPerformance.getWriters().getTxStarted() + 1);
        });
    }


    public void txAborted() {
        inLock(() -> {
            streamPerformance.getWriters().setTxAborted(streamPerformance.getWriters().getTxAborted() + 1);
        });
    }

    public void txAbortedRead() {
        inLock(() ->
            streamPerformance.getReaders().setTxAbortedRead(streamPerformance.getReaders().getTxAbortedRead() + 1)
        );
    }

    public void txCommited() {
        inLock(() -> {
            streamPerformance.getWriters().setTxCommitted(streamPerformance.getWriters().getTxCommitted() + 1);
        });
    }

    public void txFailed() {
        inLock(() -> {
            streamPerformance.getWriters().setTxFailed(streamPerformance.getWriters().getTxFailed() + 1);
        });
    }

    private void sendPerformance() {
        inLock(() -> {
            try {
                PerformanceUtils.updatePerSecondValues(streamPerformance, secondsRunning());

                PerformancePayload payload = new PerformancePayload();
                payload.setStartTime(startMillis);
                payload.setCurrentTime(System.currentTimeMillis());
                payload.setStreamName(streamName);
                payload.setStreamPerformance(streamPerformance);

                PerformanceUtils.dumpPayload("TASK", payload);
                messageClient.postPerformance(payload);
            } catch (Throwable e) {
                LOG.error("Error sending Performance Stats", e);
            }
        });
    }

    private void inLock(Runnable r) {
        synchronized (streamPerformance) {
            r.run();
        }
    }

    private void addDead(PerformanceStats perfStats, int count) {
        perfStats.getWorkerStats().setDead(perfStats.getWorkerStats().getDead()+count);
    }

    private void addCount(PerformanceStats perfStats, int count) {
        perfStats.getWorkerStats().setCount(perfStats.getWorkerStats().getCount()+count);
    }

    private void addActive(PerformanceStats perfStats, int count) {
        perfStats.getWorkerStats().setActive(perfStats.getWorkerStats().getActive()+count);
    }

    private void addEvents(PerformanceStats perfStats, long events) {
        perfStats.setEvents(perfStats.getEvents()+events);
    }

    private void addBytes(PerformanceStats perfStats, long bytes) {
        perfStats.setBytes(perfStats.getBytes()+bytes);
    }

    private long secondsRunning() {
        return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMillis);
    }
}
