package io.pravega.smoketest.model.performance;

public class AssertionPerformance {
    private boolean succeeded;
    private String expected;

    public AssertionPerformance() {}

    public AssertionPerformance(boolean succeeded, String expected) {
        this.succeeded = succeeded;
        this.expected = expected;
    }

    public boolean getSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public String getExpected() {
        return expected;
    }

    public void setExpected(String expected) {
        this.expected = expected;
    }
}
