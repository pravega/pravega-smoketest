package io.pravega.smoketest.testworker.workers;

import java.util.HashMap;
import java.util.Map;

/**
 * Pravega guarantees that events written to a particular key will be read in the same order.
 *
 * To validate this we add a SenderId and SequenceNumber (always increasing integer) to each payload.  When the payloads
 * are read this class is used to check the payload sequence number for the SenderId against the last received sequence number
 * to validate it's always increasing.
 *
 * The SenderId isn't just the key because we COULD have multiple tasks all sending to a particular key but with different
 * sequence numbers (since they aren't coordinated across the cluster).  To handle this, the SenderId is the key with the UUID
 * of the particular task that wrote the event.
 */
public class SequenceValidator {
    private Map<String, Long> sequenceCounts = new HashMap<>();

    public static String createSenderId(String key, String taskId) {
        return String.format("%s-%s", key, taskId);
    }

    public synchronized void validate(String senderId, Long sequenceNumber) {
        Long lastSequenceNumber = sequenceCounts.getOrDefault(senderId, 0L);

        if (sequenceNumber <= lastSequenceNumber) {
            throw new IllegalStateException(String.format("Event out of sequence %s: lastSeq: %d, currentSeq:%d", senderId, lastSequenceNumber, sequenceNumber));
        }
        else {
            sequenceCounts.put(senderId, sequenceNumber);
        }
    }
}