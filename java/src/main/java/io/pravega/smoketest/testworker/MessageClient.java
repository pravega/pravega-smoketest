package io.pravega.smoketest.testworker;

import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.payload.TaskParametersPayload;
import io.pravega.smoketest.utils.restclient.RestClient;
import io.pravega.smoketest.utils.restclient.RestClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageClient {
    private static final String BASE_ENDPOINT = "/v1/smoketest";

    private static final Logger LOG = LoggerFactory.getLogger(MessageClient.class);
    private RestClient restClient;

    public MessageClient() {
        String testManagerHost = System.getenv("TEST_MANAGER_HOST");
        if (testManagerHost == null) {
            LOG.error("Environment TEST_MANAGER_HOST cannot be null.");
            return;
        }

        restClient = new RestClient(RestClientUtil.defaultClient(LOG).target(testManagerHost + BASE_ENDPOINT));
    }

    public TaskParametersPayload getReady() {
        return restClient.get("/ready", TaskParametersPayload.class);
    }

    public void postPerformance(PerformancePayload payload) {
        restClient.post("/performance", payload);
    }

    public void postError(ErrorPayload payload) {
        restClient.post("/error", payload);
    }
}
