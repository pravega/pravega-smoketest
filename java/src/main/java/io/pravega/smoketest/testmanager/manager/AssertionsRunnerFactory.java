package io.pravega.smoketest.testmanager.manager;

import io.pravega.smoketest.model.performance.TestRuntime;

public abstract class AssertionsRunnerFactory {
    public static AssertionsRunner newRunner(TestRuntime runtime, TestRuntime previousRuntime) {
        return new AssertionsRunner(runtime, previousRuntime);
    }
}
