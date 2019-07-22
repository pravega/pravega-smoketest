package io.pravega.smoketest.model.payload;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class ErrorPayload {
    private String message;
    private String stacktrace;
    private String logsFilename;

    public ErrorPayload(Throwable e) {
        message = e.getMessage();
        stacktrace = ExceptionUtils.getStackTrace(e);
    }

    public ErrorPayload() {
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStacktrace() {
        return stacktrace;
    }

    public void setStacktrace(String stacktrace) {
        this.stacktrace = stacktrace;
    }

    public String getLogsFilename() {
        return logsFilename;
    }

    public void setLogsFilename(String logsFilename) {
        this.logsFilename = logsFilename;
    }
}
