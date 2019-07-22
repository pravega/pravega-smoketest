package io.pravega.smoketest.utils;

import io.pravega.smoketest.model.StreamPolicies;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.DefaultCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;

public class PravegaStreamHelper {
    private static final Logger LOG = LoggerFactory.getLogger(PravegaStreamHelper.class);
    private static final long GB_BYTES = 1_024 * 1_024 * 1_024;
    private static final long DEFAULT_RETENTION_SIZE = 50 * GB_BYTES;
    private static final RetentionPolicy DEFAULT_RETENTION_POLICY = RetentionPolicy.bySizeBytes(DEFAULT_RETENTION_SIZE);
    private final URI controllerUri;

    @Inject
    public PravegaStreamHelper(@Named("controllerUri") URI controllerUri) {
        this.controllerUri = controllerUri;
    }

    public void createScope(String scopeName) {
        LOG.info("Creating scope {}", scopeName);
        StreamManager streamManager = streamManager();
        streamManager.createScope(scopeName);
    }

    /**
     * Creates a stream with the given configuration. If the ScalingPolicy or RetentionPolicy is null,
     * it will use a default scaling or retention policy
     * */
    public void createStream(int segments, String scopeName, String streamName, StreamPolicies streamPolicies) {
        StreamManager streamManager = streamManager();

        streamManager.createScope(scopeName);
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(segments);
        RetentionPolicy retentionPolicy = DEFAULT_RETENTION_POLICY;

        if (streamPolicies != null) {
            scalingPolicy = streamPolicies.getScalingPolicy(scalingPolicy);
            retentionPolicy = streamPolicies.getRetentionPolicy(retentionPolicy);
        }

        LOG.info("Creating Stream {}/{} with {} minimal segments", scopeName, streamName, scalingPolicy.getMinNumSegments());

        StreamConfiguration streamConfig = StreamConfiguration.builder()
            .scope(scopeName)
            .streamName(streamName)
            .scalingPolicy(scalingPolicy)
            .retentionPolicy(retentionPolicy)
            .build();

        streamManager.createStream(scopeName, streamName, streamConfig);

        LOG.info("Created Stream {}/{} with {} minimal segments", scopeName, streamName, scalingPolicy.getMinNumSegments());
    }

    public void deleteStream(String scopeName, String streamName) {
        StreamManager streamManager = streamManager();

        streamManager.sealStream(scopeName, streamName);
        streamManager.deleteStream(scopeName, streamName);
        streamManager.deleteScope(scopeName);
    }

    public static Credentials adminCredentials() {
        return new DefaultCredentials("1111_aaaa", "admin");
    }

    private StreamManager streamManager() {
        return StreamManager.create(ClientConfig.builder()
            .credentials(adminCredentials())
            .controllerURI(controllerUri)
            .build());
    }
}
