package io.pravega.smoketest.model.performance;

import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.model.TestState;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TestRuntime {
    private String id;
    private String name;
    private long minutesRunning;
    private String humanRunning;
    private Long minutesLeft;
    private String humanLeft;
    private TestState state;
    private Map<String, StreamPerformance> streams = new HashMap<>();
    private AssertionResults assertionResults;
    private List<ErrorPayload> errors = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getMinutesRunning() {
        return minutesRunning;
    }

    public void setMinutesRunning(long minutesRunning) {
        this.minutesRunning = minutesRunning;
    }

    public String getHumanRunning() {
        return humanRunning;
    }

    public void setHumanRunning(String humanRunning) {
        this.humanRunning = humanRunning;
    }

    public Long getMinutesLeft() {
        return minutesLeft;
    }

    public void setMinutesLeft(Long minutesLeft) {
        this.minutesLeft = minutesLeft;
    }

    public String getHumanLeft() {
        return humanLeft;
    }

    public void setHumanLeft(String humanLeft) {
        this.humanLeft = humanLeft;
    }

    public TestState getState() {
        return state;
    }

    public void setState(TestState state) {
        this.state = state;
    }

    public Map<String, StreamPerformance> getStreams() {
        return streams;
    }

    public void setStreams(Map<String, StreamPerformance> streams) {
        this.streams = streams;
    }

    public void addStream(String stream, StreamPerformance stats) {
        this.streams.put(stream, stats);
    }

    public void setAssertionResults(AssertionResults assertionResults) {
        this.assertionResults = assertionResults;
    }

    public AssertionResults getAssertionResults() {
        return assertionResults;
    }

    public List<ErrorPayload> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorPayload> errors) {
        this.errors = errors;
    }
}
