package io.pravega.smoketest.model.payload;

import io.pravega.smoketest.model.performance.StreamPerformance;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PerformancePayload {
    private long startTime;
    private long currentTime;
    private String streamName;

    private StreamPerformance streamPerformance;

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    public StreamPerformance getStreamPerformance() {
        return streamPerformance;
    }

    public void setStreamPerformance(StreamPerformance streamPerformance) {
        this.streamPerformance = streamPerformance;
    }
}
