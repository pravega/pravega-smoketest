package io.pravega.smoketest.testworker.workers;

import java.io.Serializable;
import java.time.Instant;

/**
 * Payload which attaches a eventTime to the payload data
 */
public class PravegaTestPayload implements Serializable {
    private Instant eventTime;
    private String payload;
    private String senderId;
    private long sequenceNumber;
    private boolean fromAbortedTransaction = false;

    public PravegaTestPayload() {
    }

    public PravegaTestPayload(Instant eventTime, String senderId, long sequenceNumber, String payload) {
        this.eventTime = eventTime;
        this.payload = payload;
        this.senderId = senderId;
        this.sequenceNumber = sequenceNumber;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public boolean isFromAbortedTransaction() {
        return fromAbortedTransaction;
    }

    public void setFromAbortedTransaction(boolean fromAbortedTransaction) {
        this.fromAbortedTransaction = fromAbortedTransaction;
    }

    public String toString() {
        return this.getClass().getSimpleName() + "(eventTime=" + eventTime.toString() +
            ", senderId=" + senderId + ", sequenceNumber=" + sequenceNumber +
            ", payload.legnth()=" + payload.length() + ")";
    }
}
