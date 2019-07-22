package io.pravega.smoketest.testworker.workers.writers;

import io.pravega.smoketest.model.ThrottleConfiguration;
import io.pravega.smoketest.testworker.workers.PravegaTestPayload;
import io.pravega.smoketest.testworker.workers.SequenceValidator;
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector;
import io.pravega.smoketest.testworker.workers.events.EventGenerator;
import io.pravega.smoketest.testworker.workers.events.GeneratedEvent;
import io.pravega.smoketest.utils.ResourceSemaphore;
import io.pravega.client.ClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Base class for Pravega Writer Workers that performs the basic stuff
 */
public class PravegaWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaWriter.class);

    private static final long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);

    private final JavaSerializer<PravegaTestPayload> SERIALIZER = new JavaSerializer<>();

    private String streamName;
    private ClientFactory clientFactory;
    protected EventStreamWriter<PravegaTestPayload> producer;
    private AtomicBoolean writerFinished = new AtomicBoolean(false);

    // We set this to the max number of outstanding acks we tolerate. We decrement on writes, increment on acks.
    // Acquiring the semaphore blocks if we have the max pending that we tolerate (its value will be <= 0)
    private ResourceSemaphore pendingAckPermits;
    private Consumer<Throwable> writerFinishedHandler;
    private TaskPerformanceCollector performanceCollector;

    private long milliSecToRun;
    private boolean runForever;

    // Flow Control
    private long throttlePeriodStart;
    private long throttleEvents;
    // Runtime Stuff
    private long startTime;

    private ThrottleConfiguration throttleConfig;

    private EventGenerator eventGenerator;
    private String taskId = UUID.randomUUID().toString();
    private Map<String, Long> sequenceCounters = new HashMap<>();
    private PravegaWriterState stateHelper;

    public PravegaWriter(PravegaWriterState stateHelper) {
        this.stateHelper = stateHelper;
    }

    /** Builder Methods **/

    public PravegaWriter withStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public PravegaWriter withClientFactory(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    public PravegaWriter withWriterFinishedHandler(Consumer<Throwable> handler) {
        this.writerFinishedHandler = handler;
        return this;
    }

    public PravegaWriter withRunForever() {
        this.runForever = true;
        return this;
    }

    public PravegaWriter withSecondsToRun(long secondsToRun) {
        this.milliSecToRun = secondsToRun * 1000;

        return this;
    }

    public PravegaWriter withPerformanceCollector(TaskPerformanceCollector performanceCollector) {
        this.performanceCollector = performanceCollector;
        return this;
    }

    public PravegaWriter withThrottleConfig(ThrottleConfiguration throttleConfig) {
        this.throttleConfig = throttleConfig;

        if (throttleConfig != null) {
            if (throttleConfig.isEventsPerSecActive()) {
                LOG.info("Throttle control active, limiting to {} events/sec", throttleConfig.getCurrentMaxEventsPerSec());
            }

            if (throttleConfig.isWaitForAck()) {
                LOG.info("Throttle control active, waiting for writer ACK");
            }
            else if (throttleConfig.isThrottleAcks()) {
                LOG.info("Throttle control active, will wait when we have {} pending ACKs", throttleConfig.getMaxOutstandingAcks());
            }
        }

        return this;
    }

    public PravegaWriter withEventGenerator(EventGenerator eventGenerator) {
        this.eventGenerator = eventGenerator;
        return this;
    }

    public void prepare() {
        EventWriterConfig eventWriterConfig = EventWriterConfig.builder()
            .transactionTimeoutTime(30_000)
            .build();
        producer = clientFactory.createEventWriter(streamName, SERIALIZER, eventWriterConfig);
        stateHelper.setProducer(producer);
    }

    public void start() {
        if (throttleConfig != null && throttleConfig.isThrottleAcks()) {
            pendingAckPermits = new ResourceSemaphore(throttleConfig.getMaxOutstandingAcks());
        }

        try {
            startTime = System.currentTimeMillis();
            CompletableFuture<Void> ackFuture = null;
            while (!isTimeUp() && !writerFinished.get() || stateHelper.isInTransaction()) {

                stateHelper.beforeWrite();
                ackFuture = writeEvent();
                if (pendingAckPermits != null) {
                    pendingAckPermits.reducePermits();
                    ackFuture.thenRun(pendingAckPermits::release);
                }

                //We'll wait for the ack if we're configured to, else we'll quickly exit this function
                stateHelper.afterWrite();
                waitForAcks();
                throttle();
            }

            producer.flush();

            //If it is a waitForAck call, wait for the ack
            if (hasAckThrottle() && ackFuture != null) {
                ackFuture.get();
            }

            finished(null);
        }
        catch(InterruptedException ignore) {
            finished(null);
        }
        catch(Throwable e) {
            finished(e);
        }
    }

    protected void throttle() {
        if (!hasThroughputThrottle()) {
            return;
        }

        throttleEvents++;

        if (throttlePeriodStart == 0) {
            throttlePeriodStart = System.currentTimeMillis();
            return;
        }

        checkEventsPerSec();
    }

    protected CompletableFuture<Void> writeEvent() {
        GeneratedEvent nextEvent = eventGenerator.nextEvent();

        long sequenceNumber = sequenceCounters.compute(nextEvent.key, (key, oldVal) -> oldVal != null ? (oldVal + 1) : 1);
        String senderId = SequenceValidator.createSenderId(taskId, nextEvent.key);

        PravegaTestPayload payload = new PravegaTestPayload(Instant.now(), senderId, sequenceNumber, nextEvent.data);

        CompletableFuture<Void> writeEvent = stateHelper.doWrite(nextEvent.key, payload);
        PravegaWriterState.TransactionMode expectedState = stateHelper.getCurrentState();

        return writeEvent.whenComplete((res, ex) -> {
            if (ex == null) {
                if (expectedState.getWillBeSuccessful()) {
                    int sizeOfEvent = io.netty.buffer.Unpooled.wrappedBuffer(SERIALIZER.serialize(payload)).readableBytes();
                    performanceCollector.eventWritten(sizeOfEvent);
                }
            }
            else {
                this.finished(ex);
            }
        });
    }

    protected void finished(Throwable e) {
        boolean wasAlreadyFinished = writerFinished.getAndSet(true);

        if (!wasAlreadyFinished) {
            if (e != null) {
                LOG.error("Writer finished with Error", e);
            } else {
                LOG.info("Writer finished");

            }

            if (writerFinishedHandler != null) {
                writerFinishedHandler.accept(e);
            }
        }
    }

    private boolean isTimeUp() {
        if (runForever) {
            return false;

        }
        return startTime + milliSecToRun < System.currentTimeMillis();
    }

    private void waitForAcks() {
        if (!hasAckThrottle()) {
            return;
        }

        // We have sufficient leeway to continue running
        if (pendingAckPermits.hasAvailablePermits()) {
            return;
        }

        long startTime = System.currentTimeMillis();
        try {
            pendingAckPermits.acquire();
        }
        catch (InterruptedException e) {
            // we may be getting the sign to terminate, let's just return and be content
            LOG.error("Interrupted while waiting for acks" , e);
            return;
        }

        try {
            long endTime = System.currentTimeMillis() - startTime;
            performanceCollector.blockedWaitingOnAck(endTime);
        }
        finally {
            pendingAckPermits.release();
        }
    }

    private void checkEventsPerSec() {
        if (!hasThroughputThrottle()) {
            return;
        }

        long throttlePeriod = System.currentTimeMillis() - throttlePeriodStart;
        if (throttlePeriod > ONE_SECOND) {
            resetThrottle();
            return;
        }

        if (throttleEvents < throttleConfig.getCurrentMaxEventsPerSec()) {
            return;
        }

        // Wait here if we've hit max events for this second
        try {
            Thread.sleep(ONE_SECOND - throttlePeriod);
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted whilst performing flow control",e);
        }
    }

    private void resetThrottle() {
        throttlePeriodStart = System.currentTimeMillis();
        throttleEvents = 0;
    }

    private boolean hasAckThrottle() {
        return throttleConfig != null && throttleConfig.isThrottleAcks();
    }

    private boolean hasThroughputThrottle() {
        return throttleConfig != null && throttleConfig.isEventsPerSecActive();
    }
}
