package io.pravega.smoketest.testmanager.manager;

import io.pravega.smoketest.model.PravegaTestConfiguration;
import io.pravega.smoketest.model.TestConfiguration;
import io.pravega.smoketest.model.TestState;
import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.payload.TaskParametersPayload;
import io.pravega.smoketest.model.performance.TestRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.FileInputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

@Singleton
public class TestRuntimeManager {
    private static final Logger LOG = LoggerFactory.getLogger(TestRuntimeManager.class);

    private final PravegaTestPreparer testPreparer;
    private final URI controllerUri;

    private TestState testState;
    private TestConfiguration testConfiguration;
    private TestPerformanceCollector performanceCollector;

    @Inject
    public TestRuntimeManager(PravegaTestPreparer pravegaTestPreparer,
                              @Named("controllerUri") URI controllerUri,
                              @Named("configPath") String configPath,
                              ObjectMapper mapper) {
        this.testPreparer = pravegaTestPreparer;
        this.controllerUri = controllerUri;
        this.testState = TestState.STARTING;

        try (FileInputStream in = new FileInputStream(configPath)) {
            testConfiguration = mapper.readValue(in, TestConfiguration.class);
        } catch (Exception e) {
            LOG.error("Could not load configuration from " + configPath);
        }

        this.performanceCollector = new TestPerformanceCollector()
                .setInitialRuntimeMillis(TimeUnit.MINUTES.toMillis(testConfiguration.getMinutes()))
                .addTestConfiguration(testConfiguration);
    }

    public TestState getTestState() {
        return testState;
    }

    public void provisionTest() {
        this.testState = TestState.PREPARING;
        performanceCollector.onStateChange(this.testState);

        testConfiguration.applyGlobalOptions();
        testConfiguration.validateConfiguration();

        if (testConfiguration instanceof PravegaTestConfiguration) {
            new Thread(() -> {
                testPreparer.prepareEnvironment((PravegaTestConfiguration) testConfiguration);

                this.testState = TestState.RUNNING;
                performanceCollector.onStateChange(this.testState);
            }).start();
        }
        else {
            throw new IllegalArgumentException("Can't handle anything but Pravega tests");
        }
    }

    public TaskParametersPayload getTaskParametersPayload() {
        TaskParametersPayload payload = (TaskParametersPayload) testConfiguration.getLaunchableTasks().get(0);
        payload.setControllerUri(controllerUri.toString());
        return payload;
    }

    public void onPerformance(PerformancePayload payload) {
        performanceCollector.onPerformance(payload);
    }

    public void onError(ErrorPayload payload) {
        performanceCollector.onError(payload);
    }

    public TestRuntime getRuntime() {
        return performanceCollector.getTestRuntime();
    }
}
