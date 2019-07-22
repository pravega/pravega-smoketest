package io.pravega.smoketest.model;

public enum ReaderType {
    TAIL(0L), // MIN_VALUE would be negative since Long is signed
    CATCHUP(Long.MAX_VALUE); // TODO: make sure MAX_VALUE is indeed what we want here, when CATCHUP gets used

    private Long startPosition;
    ReaderType(Long startPosition) {
        this.startPosition = startPosition;
    }

    public Long getStartPosition() {
        return startPosition;
    }
}
