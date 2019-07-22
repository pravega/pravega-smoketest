package io.pravega.smoketest.testworker;

import io.pravega.smoketest.model.payload.PravegaTaskParametersPayload;
import io.pravega.smoketest.model.PravegaTaskConfiguration;
import io.pravega.smoketest.testworker.workers.readers.ReaderWorkerManager;
import io.pravega.smoketest.testworker.workers.writers.WriterWorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PravegaTaskDriver extends TaskDriver {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaTaskDriver.class);
    private PravegaTaskConfiguration taskConfiguration;

    private MessageClient messageClient;

    private boolean readersFinished = false;
    private boolean writersFinished = false;

    private ReaderWorkerManager readerManager = null;
    private WriterWorkerManager writerManager = null;

    private final String controllerUri;

    private PravegaTaskParametersPayload taskParameters;

    public PravegaTaskDriver(MessageClient messageClient, PravegaTaskParametersPayload taskParameters) {
        super(messageClient);
        this.taskConfiguration = taskParameters.getTaskConfiguration();

        this.taskParameters = taskParameters;
        this.controllerUri = taskParameters.getControllerUri();

        this.messageClient = messageClient;
    }

    @Override
    public void kill() {
        LOG.info("Killing all workers");

        getPerformanceCollector().stop();

        if (readerManager != null) {
            readerManager.kill();
        }

        if (writerManager != null) {
            writerManager.kill();
        }

        LOG.info("All Workers killed");
    }

    @Override
    public void onAbortMessage() {
        if (readerManager != null) {
            LOG.info("Aborting Readers");
            readerManager.abort();
        }

        if (writerManager != null) {
            LOG.info("Aborting Writers");
            writerManager.abort();
        }

        super.onAbortMessage();
    }

    /**
     * Get all worker tasks ready to start (i.e. create required Pravega managers etc)
     */
    @Override
    protected void prepare() {
        LOG.info("Preparing workers");

        if (taskConfiguration.getNumWriters() > 0) {
            LOG.info("Preparing Writer Workers");

            writerManager = new WriterWorkerManager(taskParameters,
                taskConfiguration,
                getPerformanceCollector(),
                messageClient,
                this::writersFinished);

            writerManager.prepare();
        } else {
            LOG.info("No Writer workers in this task");
            writersFinished = true;
        }

        if (taskConfiguration.getNumReaders() + taskConfiguration.getNumForgetfulReaders() > 0) {
            LOG.info("Preparing Reader Workers");

            readerManager = new ReaderWorkerManager(
                controllerUri,
                taskConfiguration,
                getPerformanceCollector(),
                messageClient,
                this::readersFinished);

            readerManager.prepare();
        } else {
            LOG.info("Preparing Reader Workers");
            readersFinished = true;
        }

        LOG.info("Prepare Finished");
    }

    /**
     * Start all workers performing their work
     */
    @Override
    protected void start() {
        LOG.info("Starting all workers");

        if (readerManager != null) {
            readerManager.start();
        }

        if (writerManager != null) {
            writerManager.start();
        }

        getPerformanceCollector().start(taskConfiguration.getStreamFQN());

        LOG.info("All Workers started");
    }

    private void writersFinished(boolean finished) {
        LOG.info("Writers have now finished their tasks");
        writersFinished = true;

        checkIfAllWorkFinished();
    }

    private void readersFinished(boolean success) {
        LOG.info("Readers have now finished their tasks");
        readersFinished = true;

        checkIfAllWorkFinished();
    }

    private void checkIfAllWorkFinished() {
        if (writersFinished && readersFinished) {
            LOG.info("All readerManager and writerManager finished");

            getPerformanceCollector().stop();

            this.getTaskFinishedHandler().invoke(null);
        }
    }
}
