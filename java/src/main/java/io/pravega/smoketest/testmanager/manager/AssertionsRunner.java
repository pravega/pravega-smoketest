package io.pravega.smoketest.testmanager.manager;

import io.pravega.smoketest.model.ReaderInfo;
import io.pravega.smoketest.model.performance.AssertionPerformance;
import io.pravega.smoketest.model.performance.AssertionResults;
import io.pravega.smoketest.model.performance.StreamPerformance;
import io.pravega.smoketest.model.performance.TestRuntime;
import com.google.common.collect.ImmutableSet;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AssertionsRunner {
    public static final Set<String> ASSERTION_KEYS = ImmutableSet.of(
        "isRunningState",
        "hasAtLeastXActiveWriters",
        "hasAtLeastXActiveReaders",
        "readersNotFallingBehind",
        "writtenAtLeastXBytes",
        "readerBytesAreIncreasing",
        "writerBytesAreIncreasing",
        "transactionsIncreasing",
        "noFailedTransaction",
        "noAbortedTransactionRead",
        "eventsInSequence"
    );

    private TestRuntime runtime;
    private TestRuntime previousRuntime;
    private Map<String, Long> smoketestAssertions;

    private AssertionResults assertionResults;
    private Map<String, AssertionPerformance> assertions;

    public AssertionsRunner(TestRuntime runtime, TestRuntime previousRuntime) {
        this.runtime = runtime;
        this.previousRuntime = previousRuntime;
        assertionResults = new AssertionResults();
        assertionResults.setSucceeded(true);
        assertions = new HashMap<>();
    }

    public AssertionsRunner setSmoketestAssertions(Map<String, Long> smoketestAssertions) {
        this.smoketestAssertions = smoketestAssertions;
        return this;
    }

    public void run() {
        checkIfRunningState();
        runtime.getStreams().forEach(this::checkAllStreamAssertions);

        long numFailedAssertions = assertions.values().stream()
                .filter(assertion -> !assertion.getSucceeded())
                .count();

        assertionResults.setSucceeded(numFailedAssertions == 0);
        assertionResults.setAssertions(assertions);
        runtime.setAssertionResults(assertionResults);
    }

    private void checkAndPutIfAtLeastValue(String key, long actual) {
        if (smoketestAssertions.containsKey(key)) {
            long expected = smoketestAssertions.get(key);
            AssertionPerformance ap = new AssertionPerformance(
                    actual >= expected,
                    "at least " + expected + ", got " + actual
            );
            assertions.put(key, ap);
        }
    }

    private void checkAndPutIfAtMostValue(String key, long actual) {
        if (smoketestAssertions.containsKey(key)) {
            long expected = smoketestAssertions.get(key);
            AssertionPerformance ap = new AssertionPerformance(
                    actual <= expected,
                    "at most " + expected + ", got " + actual
            );
            assertions.put(key, ap);
        }
    }

    private void checkAndPutIfZero(String key, long actual) {
        if (smoketestAssertions.containsKey(key)) {
            AssertionPerformance ap = new AssertionPerformance(
                actual == 0,
                0 + ", got " + actual
            );
            assertions.put(key, ap);
        }
    }

    private void checkAndPutIfEqualStrings(String key, String expected, String actual) {
        if (smoketestAssertions.containsKey(key)) {
            AssertionPerformance ap = new AssertionPerformance(
                actual.equals(expected),
                "'" + expected + "', got '" + actual + "'"
            );
            assertions.put(key, ap);
        }
    }

    private void checkAndPutBytesIncreasing(String key, long previous, long current) {
        checkAndPutValueIncreasing(key, previous, current, "bytes");
    }

    private void checkAndPutValueIncreasing(String key, long previous, long current, String label) {
        if (!smoketestAssertions.containsKey(key)) {
            return;
        }

        AssertionPerformance ap = new AssertionPerformance(
            current > previous,
            String.format("%s increase, previous: %d, current: %d", label, previous, current)
        );
        assertions.put(key, ap);
    }

    private void checkAndPutValueIncreasing(String key, Set<String> groups,
                                            Function<String, Long> previous, Function<String, Long> current,
                                            String label) {
        if (!smoketestAssertions.containsKey(key)) {
            return;
        }

        boolean didItSucceed = groups.stream().allMatch(
            (groupName) -> current.apply(groupName) > previous.apply(groupName)
        );

        String summaryMessage = String.format("%s increase\n", label) + groups.stream().map(
            (groupName) -> String.format("Group: %s, previous: %d, current: %d", groupName, previous.apply(groupName), current.apply(groupName))
        ).collect(Collectors.joining("\n"));

        AssertionPerformance ap = new AssertionPerformance(
            didItSucceed,
            summaryMessage
        );
        assertions.put(key, ap);
    }

    private void checkAllStreamAssertions(String name, StreamPerformance stream) {
        if (stream != null && stream.getWriters() != null && stream.getReaders() != null) {
            checkIfHasAtLeastXActiveWriters(stream);
            checkIfHasAtLeastXActiveReaders(stream);
            checkIfWrittenAtLeastXBytes(stream);
            checkThatReadersAreNotLagging(stream);
            checkIfReaderBytesAreIncreasing(name, stream);
            checkIfWriterBytesAreIncreasing(name, stream);
            checkIfTransactionsAreIncreasing(name, stream);
            checkNoFailedTransactions(stream);
            checkNoAbortedTransactionsRead(stream);
            checkIfEventsInSequence(name, stream);
        }
    }

    private void checkIfRunningState() {
        String actual = runtime.getState().name();
        checkAndPutIfEqualStrings("isRunningState", "RUNNING", actual);
    }

    private void checkIfHasAtLeastXActiveWriters(StreamPerformance stream) {
        long actual = (long) stream.getWriters().getWorkerStats().getActive();
        checkAndPutIfAtLeastValue("hasAtLeastXActiveWriters", actual);
    }

    private void checkIfHasAtLeastXActiveReaders(StreamPerformance stream) {
        long actual = (long) stream.getReaders().getWorkerStats().getActive();
        checkAndPutIfAtLeastValue("hasAtLeastXActiveReaders", actual);
    }

    private void checkIfWrittenAtLeastXBytes(StreamPerformance stream) {
        long actual = stream.getWriters().getBytes();
        checkAndPutIfAtLeastValue("writtenAtLeastXBytes", actual);
    }

    private void checkThatReadersAreNotLagging(StreamPerformance stream) {
        Duration furthestBehind = stream.getReaders()
            .getReaders().values().stream()
            .filter(reader -> !reader.getForgetful())
            .map(ReaderInfo::getLag)
            .max(Comparator.naturalOrder())
            .orElse(Duration.ZERO);
        checkAndPutIfAtMostValue("readersNotFallingBehind", furthestBehind.getSeconds());
    }

    private void checkIfReaderBytesAreIncreasing(String name, StreamPerformance stream) {
        StreamPerformance previousStream = previousRuntime.getStreams().get(name);
        Set<String> readerGroups = new HashSet<>(previousStream.getReaderGroups().keySet());
        readerGroups.addAll(stream.getReaderGroups().keySet());

        Function<String, Long> previousReaderGroupBytes = (readerGroup) -> previousStream.getReaderGroup(readerGroup).getBytes();
        Function<String, Long> readerGroupBytes = (readerGroup) -> stream.getReaderGroup(readerGroup).getBytes();

        checkAndPutValueIncreasing("readerBytesAreIncreasing",
            readerGroups,
            previousReaderGroupBytes,
            readerGroupBytes,
            "bytes");
    }

    private void checkIfWriterBytesAreIncreasing(String name, StreamPerformance stream) {
        StreamPerformance previousStream = previousRuntime.getStreams().get(name);
        checkAndPutBytesIncreasing("writerBytesAreIncreasing",
            previousStream.getWriters().getBytes(),
            stream.getWriters().getBytes());
    }

    private void checkIfTransactionsAreIncreasing(String name, StreamPerformance stream) {
        StreamPerformance previousStream = previousRuntime.getStreams().get(name);
        checkAndPutValueIncreasing("transactionsIncreasing",
            previousStream.getWriters().getTxCommitted(),
            stream.getWriters().getTxCommitted(), "transactions");
    }

    private void checkNoAbortedTransactionsRead(StreamPerformance stream) {
        long actual = stream.getReaders().getTxAbortedRead();
        checkAndPutIfZero("noAbortedTransactionRead", actual);
    }

    private void checkNoFailedTransactions(StreamPerformance stream) {
        long actual = stream.getWriters().getTxFailed();
        checkAndPutIfZero("noFailedTransaction", actual);
    }

    private void checkIfEventsInSequence(String name, StreamPerformance stream) {
        long actual = stream.getReaders().getEventsOutOfSequence();
        checkAndPutIfZero("eventsInSequence", actual);
    }
}
