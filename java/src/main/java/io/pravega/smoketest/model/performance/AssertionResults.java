package io.pravega.smoketest.model.performance;

import java.util.Map;

public class AssertionResults {
    private boolean succeeded;
    private Map<String, AssertionPerformance> assertions;

    public boolean getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public Map<String, AssertionPerformance> getAssertions() {
        return assertions;
    }

    public void setAssertions(Map<String, AssertionPerformance> assertions) {
        this.assertions = assertions;
    }
}
