package io.pravega.smoketest.testmanager.resources;

import io.pravega.smoketest.model.TestState;
import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.performance.TestRuntime;
import io.pravega.smoketest.testmanager.manager.TestRuntimeManager;
import io.pravega.smoketest.utils.ErrorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/v1/smoketest")
@Produces(MediaType.APPLICATION_JSON)
public class ManagerResource {
    private static final Logger LOG = LoggerFactory.getLogger(ManagerResource.class);

    private final TestRuntimeManager testRuntimeManager;

    @Inject
    public ManagerResource(TestRuntimeManager testRuntimeManager) {
        this.testRuntimeManager = testRuntimeManager;
    }

    @Path("/ready")
    @GET
    public Response getTest() {
        if (testRuntimeManager.getTestState() == TestState.STARTING) {
            testRuntimeManager.provisionTest();
            return ErrorBuilder.serviceUnavailable();
        }
        else if (testRuntimeManager.getTestState() == TestState.PREPARING) {
            return ErrorBuilder.serviceUnavailable();
        }
        else {
            return Response.ok(testRuntimeManager.getTaskParametersPayload()).build();
        }
    }

    @Path("/runtime")
    @GET
    public TestRuntime getRuntime() {
        return testRuntimeManager.getRuntime();
    }

    @Path("/performance")
    @POST
    public PerformancePayload postPerformance(PerformancePayload payload) {
        testRuntimeManager.onPerformance(payload);
        return payload;
    }

    @Path("/error")
    @POST
    public ErrorPayload postError(ErrorPayload payload) {
        testRuntimeManager.onError(payload);
        return payload;
    }
}
