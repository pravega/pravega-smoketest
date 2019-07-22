package io.pravega.smoketest.model;

public enum TestState {
    STARTING(false),

    // Test Environment is being prepared
    PREPARING(false),

    // All tasks are now staged and are running
    RUNNING(false),

    // Task completed successfully
    FINISHED(true),

    // Task was manually stopped before completion
    STOPPED(true);

    boolean finishedState;

    TestState(boolean finishedState) {
        this.finishedState = finishedState;
    }

    public boolean isFinishedState() {
        return finishedState;
    }
}
