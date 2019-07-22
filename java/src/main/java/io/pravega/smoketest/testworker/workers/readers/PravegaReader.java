package io.pravega.smoketest.testworker.workers.readers;

import io.pravega.smoketest.model.ReaderInfo;
import io.pravega.smoketest.testworker.workers.PravegaTestPayload;
import io.pravega.smoketest.testworker.workers.SequenceValidator;
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector;
import io.pravega.smoketest.utils.PravegaNamingUtils;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PravegaReader {
    private static Logger LOG = LoggerFactory.getLogger(PravegaReader.class);

    private final JavaSerializer<PravegaTestPayload> SERIALIZER = new JavaSerializer<>();

    private String readerId;
    private ClientFactory clientFactory;
    private EventStreamReader<PravegaTestPayload> eventStreamReader;

    private String stream;

    private Consumer<ReaderFinishedEvent> readerFinishedHandler = (i) -> {};
    private boolean stopped;
    private TaskPerformanceCollector performanceCollector;
    private ReaderGroup readerGroup;

    private Position lastPosition;
    private SequenceValidator sequenceValidator;
    private boolean forgetful = false;
    private long attentionSpanMinutes = TimeUnit.HOURS.toMinutes(12);
    private LocalDateTime startTime = LocalDateTime.now();

    public PravegaReader() {
    }

    public PravegaReader withClientFactory(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    public PravegaReader withReaderGroup(ReaderGroup readerGroup) {
        this.readerGroup = readerGroup;
        return this;
    }

    public PravegaReader withStream(String stream) {
        this.stream = stream;
        return this;
    }

    public PravegaReader withAttentionSpan(long attentionSpanMinutes) {
        this.attentionSpanMinutes = attentionSpanMinutes;
        this.forgetful = true;
        return this;
    }

    public PravegaReader withFinishedHandler(Consumer<ReaderFinishedEvent> finishedHandler) {
        this.readerFinishedHandler =  ObjectUtils.defaultIfNull(finishedHandler, i -> {});
        return this;
    }

    public PravegaReader withPerformanceCollector(TaskPerformanceCollector performanceCollector) {
        this.performanceCollector = performanceCollector;
        return this;
    }

    public void prepare() {
        sequenceValidator = new SequenceValidator();

        ReaderConfig readerConfig = ReaderConfig.builder()
            .build();

        readerId = PravegaNamingUtils.newReaderId(readerGroup.getScope(), stream);
        eventStreamReader = clientFactory.createReader(readerId, readerGroup.getGroupName(), SERIALIZER, readerConfig);

        LOG.info("Added Reader {} To ReaderGroup {}", readerId, readerGroup.getGroupName());
    }

    public void start() {
        try {
            while (!stopped) {
                try {
                    beforeRead();
                    EventRead<PravegaTestPayload> result = eventStreamReader.readNextEvent(1000);
                    lastPosition = result.getPosition();

                    if (result.getEvent() != null) {
                        afterRead(result.getEvent());
                    }
                }
                catch (TruncatedDataException truncatedException) {
                    if (!this.forgetful) {
                        throw truncatedException;
                    }
                    LOG.error("Got a truncated data exception on forgetful reader {} after position {}", readerId, lastPosition, truncatedException);
                }
            }

            finished(null);
        }
        catch (Throwable e) {
            finished(e);
        }
    }

    private void beforeRead() {
        long minutesElapsed = startTime.until(LocalDateTime.now(), ChronoUnit.MINUTES);
        if (minutesElapsed >= this.attentionSpanMinutes && this.forgetful) {
            startTime = LocalDateTime.now();

            // reset ReaderGroup
            ReaderGroupConfig resetConfig = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream(readerGroup.getScope() + "/" + stream)
                .build();
            LOG.info("Resetting ReaderGroup {} at position {}", readerGroup.getGroupName(), lastPosition);
            readerGroup.resetReaderGroup(resetConfig);

            // Reinitialize reader; we need to recreate the reader since the group got mutated.
            // This means 'forgetful' readers need to be in a reader group by themselves
            // TODO add support for groups of forgetful readers
            prepare();
        }
    }

    public void stop() {
        stopped = true;
    }

    public void afterRead(PravegaTestPayload payload) {
        if (performanceCollector != null) {
            int sizeOfEvent = io.netty.buffer.Unpooled.wrappedBuffer(SERIALIZER.serialize(payload)).readableBytes();
            ReaderInfo snapshotInfo = new ReaderInfo(this.hashCode() + "",
                readerGroup.getGroupName(), payload.getEventTime(),
                sizeOfEvent, this.forgetful);

            performanceCollector.eventRead(snapshotInfo);

            validateTransactionState(payload);
            validateSequence(payload);
        }
    }

    public void finished(Throwable e) {
        if (e != null) {
            LOG.error("Reader finished with Error", e);
        }
        else {
            LOG.info("Reader finished");
        }

        try {
            readerGroup.readerOffline(readerId, lastPosition);
            LOG.info("Removed Reader {} from ReaderGroup {} at position {}", readerId, readerGroup.getGroupName(), lastPosition);
        } catch (Throwable cleanupException) {
            LOG.error("Couldn't cleanup finished reader {} at position {}", readerId, lastPosition, cleanupException);
        }
        finally {
            readerFinishedHandler.accept(new ReaderFinishedEvent(e));
        }
    }

    private void validateTransactionState(PravegaTestPayload payload) {
        if (payload.isFromAbortedTransaction()) {
            LOG.error("Got an aborted transaction's write (sender, sequence): {} {}",
                payload.getSenderId(), payload.getSequenceNumber());

            performanceCollector.txAbortedRead();
        }
    }

    private void validateSequence(PravegaTestPayload payload) {
        try {
            sequenceValidator.validate(payload.getSenderId(), payload.getSequenceNumber());
        }
        catch(IllegalStateException e) {
            LOG.error(e.getMessage());
            performanceCollector.eventOutOfSequence(readerGroup.getGroupName());
        }
    }

    public ReaderGroup getReaderGroup() {
        return readerGroup;
    }

    public class ReaderFinishedEvent {
        private Throwable rootError;

        public ReaderFinishedEvent(Throwable rootError) {
            this.rootError = rootError;
        }

        public Throwable getRootError() {
            return rootError;
        }

        public PravegaReader getFromReader() {
            return PravegaReader.this;
        }
    }

}
