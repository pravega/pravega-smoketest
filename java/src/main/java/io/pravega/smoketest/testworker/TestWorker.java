package io.pravega.smoketest.testworker;

import io.pravega.smoketest.model.payload.PravegaTaskParametersPayload;
import io.pravega.smoketest.model.payload.TaskParametersPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TestWorker.class);
    private static MessageClient messageClient = new MessageClient();

    public static void main(String[] args) {
        try {
            TaskDriver taskDriver;

            TaskParametersPayload paramPayload = getTaskParametersPayload();
            if (paramPayload instanceof PravegaTaskParametersPayload) {
                taskDriver = new PravegaTaskDriver(messageClient, (PravegaTaskParametersPayload) paramPayload);
            } else {
                throw new IllegalArgumentException("Can't handle anything but Pravega tests");
            }

            taskDriver.run();
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }

    private static TaskParametersPayload getTaskParametersPayload() {
        while (true) {
            try {
                Thread.sleep(2000);
                return messageClient.getReady();
            } catch (Exception e) {
                LOG.error(e.toString());
            }
        }
    }
}
