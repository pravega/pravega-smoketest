package io.pravega.smoketest.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;

import java.time.Duration;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamPolicies {
    public enum ScalingType {
        BYTES_PER_SECOND, EVENTS_PER_SECOND, FIXED_NUM_SEGMENTS 
    }

    public enum RetentionType {
        DISABLED, LIMITED_TIME_MILLIS, LIMITED_SIZE_BYTES 
    }

    private RetentionType retentionType;
    private Integer scaleFactor;
    private Integer targetRate;
    private int minNumSegments;

    private ScalingType scalingType;
    private Integer retentionLimit;

    public ScalingType getScalingType() {
        return scalingType;
    }

    public void setScalingType(ScalingType scalingType) {
        this.scalingType = scalingType;
    }

    public RetentionType getRetentionType() {
        return retentionType;
    }

    public void setRetentionType(RetentionType retentionType) {
        this.retentionType = retentionType;
    }

    public Integer getScaleFactor() {
        return scaleFactor;
    }

    public void setScaleFactor(Integer scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    public Integer getTargetRate() {
        return targetRate;
    }

    public void setTargetRate(Integer targetRate) {
        this.targetRate = targetRate;
    }

    public Integer getRetentionLimit() {
        return retentionLimit;
    }

    public void setRetentionLimit(Integer retentionLimit) {
        this.retentionLimit = retentionLimit;
    }

    public int getMinNumSegments() {
        return minNumSegments;
    }

    public void setMinNumSegments(int numSegments) {
        this.minNumSegments = numSegments;
    }

    @JsonIgnore
    public ScalingPolicy getScalingPolicy(ScalingPolicy defaultIfNone) {
        if (this.getScalingType() == null) {
            return defaultIfNone;
        }

        switch (this.getScalingType()) {
            case BYTES_PER_SECOND:
                return ScalingPolicy.byDataRate(this.getTargetRate(), this.getScaleFactor(), this.getMinNumSegments());
            case EVENTS_PER_SECOND:
                return ScalingPolicy.byEventRate(this.getTargetRate(), this.getScaleFactor(), this.getMinNumSegments());
            case FIXED_NUM_SEGMENTS:
                return ScalingPolicy.fixed(this.getMinNumSegments());
        }
        return ScalingPolicy.fixed(this.getMinNumSegments());
    }

    @JsonIgnore
    public RetentionPolicy getRetentionPolicy(RetentionPolicy defaultIfNone) {
        if (this.getRetentionType() == null) {
            return defaultIfNone;
        }
        
        switch (this.getRetentionType()) {
            case DISABLED:
                return null;
            case LIMITED_SIZE_BYTES:
                return RetentionPolicy.bySizeBytes(this.retentionLimit);
            case LIMITED_TIME_MILLIS:
                return RetentionPolicy.byTime(Duration.ofMillis(this.retentionLimit));
        }
        return null;
    }
}
