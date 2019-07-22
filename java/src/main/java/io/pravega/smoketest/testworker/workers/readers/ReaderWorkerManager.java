package io.pravega.smoketest.testworker.workers.readers;

import io.pravega.smoketest.model.PravegaTaskConfiguration;
import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.testworker.MessageClient;
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.pravega.smoketest.utils.PravegaStreamHelper.adminCredentials;

/**
 * Manages a Pool of Reader tasks performing Read operations against a Pravega Instance
 */
public class ReaderWorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderWorkerManager.class);
    /**
     *
     * These will be configurable through TestConfiguration
     *
     */

    private ClientFactory clientFactory;

    private final String scopeName;
    private final String streamName;
    private final URI controllerUri;
    private final int numReaders;
    private final int numForgetfulReaders;
    private final long attentionSpan;

    private long minutesToRun;
    private boolean forever;
    private final String readerGroupName;

    private final ExecutorService executor;

    private final Consumer finishedCallback;

    private CountDownLatch finishedCountdown;
    private ReaderGroupManager readerGroupManager;
    private TaskPerformanceCollector performanceCollector;

    private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(5);
    private List<PravegaReader> readers = new ArrayList<>();
    private boolean prepared = false;
    private int deployedReader = 0;

    private MessageClient messageClient;

    public ReaderWorkerManager(String controllerUri,
                               PravegaTaskConfiguration taskConfiguration,
                               TaskPerformanceCollector performanceCollector,
                               MessageClient messageClient,
                               Consumer<Boolean> finishedCallback) {

        this.scopeName = taskConfiguration.getScope();
        this.streamName = taskConfiguration.getStream();
        this.controllerUri = URI.create(controllerUri);
        this.numReaders = taskConfiguration.getNumReaders();
        this.numForgetfulReaders = taskConfiguration.getNumForgetfulReaders();
        this.attentionSpan = taskConfiguration.getAttentionSpan();
        this.readerGroupName = taskConfiguration.getReaderGroup();
        this.minutesToRun = taskConfiguration.getMinutes();
        this.forever = taskConfiguration.getForever();

        this.executor = Executors.newCachedThreadPool();
        this.messageClient = messageClient;
        this.finishedCallback = finishedCallback;
        this.performanceCollector = performanceCollector;
    }

    public void prepare() {
        LOG.info("Starting {} readers on stream {}/{}", numReaders + numForgetfulReaders, scopeName, streamName);

        ClientConfig clientConfig = ClientConfig.builder()
            .credentials(adminCredentials())
            .controllerURI(controllerUri).build();
        clientFactory = ClientFactory.withScope(scopeName, clientConfig);
        readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);

        finishedCountdown = new CountDownLatch(numReaders + numForgetfulReaders);
        makeReaders();
        makeForgetfulReaders();
        prepared = true;
    }

    public void makeReaders() {
        if (numReaders > 0) {
            ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);
            makeReadersInGroup(numReaders, readerGroup, (i) -> {});
        }
    }

    private void makeForgetfulReaders() {
        Consumer<PravegaReader> prepTimespan = (reader) -> reader.withAttentionSpan(attentionSpan);
        for (int forgetfulReaderNum = 0; forgetfulReaderNum < numForgetfulReaders; forgetfulReaderNum++) {
            ReaderGroup readerGroup = makeForgetfulReaderGroup(forgetfulReaderNum);
            makeReadersInGroup(1, readerGroup, prepTimespan);
        }
    }

    private void makeReadersInGroup(int numToUse, ReaderGroup groupToUse, Consumer<PravegaReader> finalPrep) {
        for (int readerNum = 0; readerNum < numToUse; readerNum++){
            PravegaReader reader = new PravegaReader()
                .withClientFactory(clientFactory)
                .withReaderGroup(groupToUse)
                .withStream(streamName)
                .withFinishedHandler(this::readerFinished)
                .withPerformanceCollector(performanceCollector);

            finalPrep.accept(reader);

            reader.prepare();
            deployedReader++;
            readers.add(reader);
            performanceCollector.readerStarted(groupToUse.getGroupName());
            LOG.info("Reader {} prepared", deployedReader);
        }
    }

    public void start() {
        if (!prepared) {
            throw new IllegalStateException("Has not been prepared");
        }

        int index = 0;
        for (PravegaReader reader : readers) {
            executor.execute(reader::start);
            LOG.info("Started reader {}",index++);
        }

        // Set a wait in case readers finish early due to errors  (finishCountdown should not be decremented before timesup!)
        executor.execute(() -> {
            try {
                finishedCountdown.await();

                finishedCallback.accept(false);
            } catch (InterruptedException e) {
                LOG.error("Error waiting for Readers to finish", e);
                ErrorPayload payload = new ErrorPayload(new RuntimeException("Error waiting for readers to finish", e));
                messageClient.postError(payload);
            }
        });

        if (forever) {
            LOG.info("Running readers FOREVER");
        }
        else {
            LOG.info("Will kill readers in {} minutes", minutesToRun);
            // Give the readers the required amount of seconds
            scheduledExecutorService.schedule(() -> {
                LOG.info("Times Up, killing all readers");

                kill();

                finishedCallback.accept(false);
            }, minutesToRun, TimeUnit.MINUTES);
        }
    }

    public void kill() {
        executor.shutdownNow();

        LOG.info("Killed all reader workers");
    }

    public void abort() {
        executor.shutdownNow();
    }

    private void readerFinished(PravegaReader.ReaderFinishedEvent e) {
        Throwable rootError = e.getRootError();
        if (rootError != null) {
            LOG.error("Reader Finished with Error", rootError);
            ErrorPayload payload = new ErrorPayload(rootError);
            messageClient.postError(payload);
        }

        ReaderGroup readersGroup = e.getFromReader().getReaderGroup();
        performanceCollector.readerEnded(readersGroup.getGroupName());
        finishedCountdown.countDown();

        LOG.info("Reader finished, {} left running", finishedCountdown.getCount());
    }

    private ReaderGroup makeForgetfulReaderGroup(int readerNum) {
        String forgetfulReaderGroup = String.format("%s%sforgetful%s", scopeName, streamName, readerNum);
        String scopedStreamName = scopeName + "/" + streamName;
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
            .stream(scopedStreamName).build();

        LOG.info("Creating reader group for forgetful reader {}", forgetfulReaderGroup);
        readerGroupManager.createReaderGroup(forgetfulReaderGroup, groupConfig);
        return readerGroupManager.getReaderGroup(forgetfulReaderGroup);
    }
}
