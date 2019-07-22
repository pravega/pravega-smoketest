package io.pravega.smoketest.testmanager.manager;

import io.pravega.smoketest.model.PravegaTaskConfiguration;
import io.pravega.smoketest.model.PravegaTestConfiguration;
import io.pravega.smoketest.model.ReaderType;
import io.pravega.smoketest.utils.PravegaStreamHelper;
import com.google.inject.name.Named;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static io.pravega.smoketest.utils.PravegaStreamHelper.adminCredentials;

@Singleton
public class PravegaTestPreparer {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaTestPreparer.class);

    private final PravegaStreamHelper streamHelper;
    private final URI controllerUri;

    @Inject
    public PravegaTestPreparer(@Named("controllerUri") URI controllerUri, PravegaStreamHelper streamHelper) {
        this.controllerUri = controllerUri;
        this.streamHelper = streamHelper;
    }

    public void prepareEnvironment(PravegaTestConfiguration testConfiguration) {
        LOG.info("Preparing environment for test {} ({})", testConfiguration.getName(), testConfiguration.getId());
        try {
            if (testConfiguration.getCreateStream()) {
                createStreams(testConfiguration);
            }

            createReaderGroups(testConfiguration);

            LOG.info("Finished environment for test {} ({})", testConfiguration.getName(), testConfiguration.getId());

        }
        catch(Exception e) {
            LOG.error("Failed preparing Test Environment",e);
            throw e;
        }
    }

    private void createStreams(PravegaTestConfiguration testConfiguration) {
        LOG.info("Creating Streams");

        Map<String, List<PravegaTaskConfiguration>> tasksByStream = testConfiguration.getTasksByStream();

        for (Map.Entry<String, List<PravegaTaskConfiguration>> entry : tasksByStream.entrySet()) {
            List<PravegaTaskConfiguration> tasksForStream = entry.getValue();
            int numReaders = entry.getValue().stream()
                .mapToInt(t -> t.getNumReaders())
                .sum();

            int segments = Math.max(1, numReaders);

            PravegaTaskConfiguration taskConfiguration = tasksForStream.get(0);

            streamHelper.createStream(segments, taskConfiguration.getScope(), taskConfiguration.getStream(), taskConfiguration.getStreamPolicies());
        }

        LOG.info("Finished Creating Streams");
    }

    private void createReaderGroups(PravegaTestConfiguration testConfiguration) {
        LOG.info("Creating Reader Groups");
        Map<String, List<PravegaTaskConfiguration>> tasksByReaderGroup = testConfiguration.getTasksByReaderGroup();

        for (Map.Entry<String, List<PravegaTaskConfiguration>> entry : tasksByReaderGroup.entrySet()) {
            int numReaders = entry.getValue().stream()
                .mapToInt(t -> t.getNumReaders())
                .sum();


            if (numReaders > 0) {
                PravegaTaskConfiguration taskConfiguration = entry.getValue().get(0);
                createReaderGroup(taskConfiguration.getReaderGroup(),
                    taskConfiguration.getReaderType(),
                    taskConfiguration.getScope(),
                    taskConfiguration.getStream());
            }
        }

        LOG.info("Finished Creating Reader Groups");
    }

    public void createReaderGroup(String readerGroupName, ReaderType readerType, String scope, String stream) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope,
            ClientConfig.builder()
                .credentials(adminCredentials())
                .controllerURI(controllerUri)
                .build());

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().stream(scope + "/" + stream).build();

        LOG.info("Creating reader group {}", readerGroupName);
        readerGroupManager.createReaderGroup(readerGroupName, groupConfig);
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupName);

        for (String readerName : readerGroup.getOnlineReaders()) {
            LOG.info("ReaderGroup {} already contained reader {}", readerGroup.getGroupName(), readerName);
        }

        // This may be a reuse of a reader group so reset it
        readerGroup.resetReaderGroup(groupConfig);

        LOG.info("Created reader group {}", readerGroupName);

    }

    public void cleanupEnvironment(PravegaTestConfiguration testConfiguration) {
        if (testConfiguration.getDeleteStream()) {
            streamHelper.deleteStream(testConfiguration.getScope(), testConfiguration.getStream());
        }
    }
}
