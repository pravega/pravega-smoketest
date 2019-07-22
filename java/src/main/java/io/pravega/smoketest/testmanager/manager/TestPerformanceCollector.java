package io.pravega.smoketest.testmanager.manager;

import io.pravega.smoketest.model.payload.ErrorPayload;
import io.pravega.smoketest.model.payload.PerformancePayload;
import io.pravega.smoketest.model.ReaderInfo;
import io.pravega.smoketest.model.TestConfiguration;
import io.pravega.smoketest.model.TestState;
import io.pravega.smoketest.model.performance.ReaderGroupPerformance;
import io.pravega.smoketest.model.performance.ReaderPerformance;
import io.pravega.smoketest.model.performance.StreamPerformance;
import io.pravega.smoketest.model.performance.TestRuntime;
import io.pravega.smoketest.model.performance.WorkerStats;
import io.pravega.smoketest.model.performance.WriterPerformance;
import io.pravega.smoketest.testworker.workers.TaskPerformanceCollector;
import io.pravega.smoketest.utils.PerformanceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Receives performance reports from Tasks running in the cluster on a periodic (5 second) basis.
 *
 * Is responsible for collating these performance reports and merging them into one TestPerformance which is complicated
 * by the fact we could have multiple tasks all reading/writing to the same stream with the same ReaderGroups
 */
public class TestPerformanceCollector {
    private static Logger LOG = LoggerFactory.getLogger(TestPerformanceCollector.class);

    private Map<String, PerformancePayload> taskToLatestPayload = new ConcurrentHashMap<>();
    private TestRuntime cachedTestRuntime = new TestRuntime();
    private TestRuntime previousTestRuntime = null;

    private long runtimeMillis;

    private long startTimeMillis;
    private long currentTimeMillis;
    private long runningMillis;
    private long runningSeconds;

    private TestConfiguration<?> testConfiguration;
    private List<ErrorPayload> errors = new ArrayList<>();

    private int autoUpdateTimeMinutes = TaskPerformanceCollector.SENDING_SCHEDULE_SECS / 60 + 2;
    private ScheduledExecutorService autoUpdateScheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> task = autoUpdateScheduler.scheduleAtFixedRate(this::autoUpdateRuntime, autoUpdateTimeMinutes, autoUpdateTimeMinutes, TimeUnit.MINUTES);

    public TestPerformanceCollector setInitialRuntimeMillis(long runtimeMillis) {
        this.runtimeMillis = runtimeMillis;
        updateRuntimeTimes(cachedTestRuntime);
        return this;
    }

    public TestPerformanceCollector addTestConfiguration(TestConfiguration<?> testConfiguration) {
        this.testConfiguration = testConfiguration;
        return this;
    }

    /**
     * Called when a performance report is received
     */
    public void onPerformance(PerformancePayload performancePayload) {
        PerformanceUtils.dumpPayload("Got new data", performancePayload);

        // We got an update so let's reschedule the auto-update
        task.cancel(false);
        task = autoUpdateScheduler.scheduleAtFixedRate(this::autoUpdateRuntime, autoUpdateTimeMinutes, autoUpdateTimeMinutes, TimeUnit.MINUTES);

        syncStartTime(performancePayload.getStartTime());
        syncCurrentTime(performancePayload.getCurrentTime());

        // Update our cache of the last performance report from this task
        taskToLatestPayload.put("taskID", performancePayload);
        updatePerformance();
    }

    public void onError(ErrorPayload error) {
        LOG.info("Received Error From task {} : {}\n{}", "taskID", error.getMessage(), error.getStacktrace());
        try {
            errors.add(error);
        } catch (Exception e) {
            LOG.error("Failed to get logs", e);
        }
    }

    public void onStateChange(TestState state) {
        // By definition, there are no active workers when a final state is reached.
        if (state.isFinishedState()) {
            cachedTestRuntime.getStreams().forEach((name, stream) -> {
                if (stream != null ) {
                    if (stream.getWriters() != null) {
                        stream.getWriters().getWorkerStats().setActive(0);
                    }

                    if (stream.getReaders() != null) {
                        stream.getReaders().getWorkerStats().setActive(0);
                        stream.getReaderGroups().values()
                            .forEach((group) -> group.getWorkerStats().setActive(0));
                    }
                }
            });
        }
        cachedTestRuntime.setState(state);
    }

    public TestRuntime getTestRuntime() {
        AssertionsRunnerFactory.newRunner(cachedTestRuntime, previousTestRuntime)
            .setSmoketestAssertions(testConfiguration.getSmoketestAssertions())
            .run();

        cachedTestRuntime.setErrors(errors);

        return cachedTestRuntime;
    }

    public void stop() {
        task.cancel(false);
    }

    private void updatePerformance() {
        // Create a new TestPerformance and merge all the cached task performance reports into it
        TestRuntime mergedRuntime = new TestRuntime();
        mergePerformanceStats(mergedRuntime);
        updateRuntimeTimes(mergedRuntime);

        // Update the test state
        mergedRuntime.setState(cachedTestRuntime.getState());

        // Update the dynamic values
        mergedRuntime.getStreams().values()
            .forEach(stream -> PerformanceUtils.updatePerSecondValues(stream, runningSeconds));

        // Keep track of an older test runtime for assertion comparisons. Only update previous
        // instance every time the minutes running changes.
        if (previousTestRuntime == null) {
            previousTestRuntime = mergedRuntime;
        }
        else if (previousTestRuntime.getMinutesRunning() != cachedTestRuntime.getMinutesRunning()) {
            previousTestRuntime = cachedTestRuntime;
        }

        // Update the Cache
        cachedTestRuntime = mergedRuntime;
        PerformanceUtils.dumpRuntime("New runtime for " + testConfiguration.getId(), cachedTestRuntime);

    }

    /**
     * In normal operations Tasks ping this class and gives it a hook to update. If those tasks are non-responsive
     * yet we don't get the TASK_ERROR, TASK_FAILED, or TASK_LOST signals, the TestRuntime won't update.
     *
     * This triggers the update. For smoketest runs with bytes increasing, this will cause a visible failure.
     */
    private void autoUpdateRuntime() {
        LOG.error("Had to force update");
        syncCurrentTime(System.currentTimeMillis());
        updatePerformance();
    }

    private void mergePerformanceStats(TestRuntime testRuntime) {
        // Group Payloads into StreamName -> Payloads
        Map<String, List<PerformancePayload>> taskToStreams = taskToLatestPayload.values().stream()
            .collect(Collectors.groupingBy(PerformancePayload::getStreamName));


        // For each list, merge all the PerformanceStats for that stream together
        for (Map.Entry<String, List<PerformancePayload>> streamEntry : taskToStreams.entrySet()) {
            List<StreamPerformance> streamPerformances = streamEntry.getValue().stream()
                .map(PerformancePayload::getStreamPerformance)
                .collect(Collectors.toList());

            // Add the final Merged Stream performance to the testPerformance
            testRuntime.getStreams().put(streamEntry.getKey(), mergedPerformances(streamPerformances));
        }

    }

    public Map<String, ReaderInfo> mergeDistinctReaderInfo(Map<String, ReaderInfo> first, Map<String, ReaderInfo> second) {
        Map<String, ReaderInfo> dest = new HashMap<>();
        dest.putAll(first);
        dest.putAll(second);
        return dest;
    }

    private StreamPerformance mergedPerformances(List<StreamPerformance> streamPerformances) {
        StreamPerformance mergedPerformance = new StreamPerformance();

        for (StreamPerformance streamPerformance : streamPerformances) {
            mergedPerformance.setWriters(mergeWriters(mergedPerformance.getWriters(), streamPerformance.getWriters()));
            mergedPerformance.setReaders(mergeReaders(mergedPerformance.getReaders(), streamPerformance.getReaders()));
        }

        return mergedPerformance;
    }

    private WriterPerformance mergeWriters(WriterPerformance writer1, WriterPerformance writer2) {
        if (writer1 == null) {
            return writer2;
        }

        if (writer2 == null) {
            return writer1;
        }

        WriterPerformance mergedStats = new WriterPerformance();
        mergedStats.setEvents(writer1.getEvents()+writer2.getEvents());
        mergedStats.setBytes(writer1.getBytes()+writer2.getBytes());

        mergedStats.setTxStarted(writer1.getTxStarted()+writer2.getTxStarted());
        mergedStats.setTxCommitted(writer1.getTxCommitted()+writer2.getTxCommitted());
        mergedStats.setTxAborted(writer1.getTxAborted()+writer2.getTxAborted());
        mergedStats.setTxFailed(writer1.getTxFailed()+writer2.getTxFailed());

        mergedStats.setWorkerStats(mergeWorkerStats(writer1.getWorkerStats(), writer2.getWorkerStats()));
        WriterPerformance.mergeBlockedStats(mergedStats, writer1, writer2);
        return mergedStats;

    }

    private ReaderPerformance mergeReaders(ReaderPerformance reader1, ReaderPerformance reader2) {
        if (reader1 == null) {
            return reader2;
        }

        if (reader2 == null) {
            return reader1;
        }

        ReaderPerformance mergedStats = new ReaderPerformance();

        mergedStats.setEvents(reader1.getEvents() + reader2.getEvents());
        mergedStats.setBytes(reader1.getBytes() + reader2.getBytes());
        mergedStats.setEventsOutOfSequence(reader1.getEventsOutOfSequence() + reader2.getEventsOutOfSequence());
        mergedStats.setTxAbortedRead(reader1.getTxAbortedRead() + reader2.getTxAbortedRead());

        mergedStats.setWorkerStats(mergeWorkerStats(reader1.getWorkerStats(), reader2.getWorkerStats()));
        mergedStats.setReaderGroups(mergedReaderGroups(reader1, reader2));

        mergedStats.setReaders(mergeDistinctReaderInfo(reader1.getReaders(),reader2.getReaders()));
        return mergedStats;
    }

    private ReaderGroupPerformance mergeReaderGroup(ReaderGroupPerformance perf1, ReaderGroupPerformance perf2) {
        ReaderGroupPerformance mergedStats = new ReaderGroupPerformance();
        mergedStats.setEvents(perf1.getEvents()+perf2.getEvents());
        mergedStats.setBytes(perf1.getBytes()+perf2.getBytes());
        mergedStats.setWorkerStats(mergeWorkerStats(perf1.getWorkerStats(), perf2.getWorkerStats()));
        mergedStats.setEventsOutOfSequence(perf1.getEventsOutOfSequence() + perf2.getEventsOutOfSequence());
        return mergedStats;
    }

    private WorkerStats mergeWorkerStats(WorkerStats worker1, WorkerStats worker2) {
        if (worker1 == null) {
            return worker2;
        }

        if (worker2 == null) {
            return worker1;
        }

        WorkerStats mergedStats = new WorkerStats();
        mergedStats.setCount(worker1.getCount() + worker2.getCount());
        mergedStats.setActive(worker1.getActive() + worker2.getActive());
        mergedStats.setIdle(worker1.getIdle() + worker2.getIdle());
        mergedStats.setDead(worker1.getDead() + worker2.getDead());

        return mergedStats;
    }

    private Map<String, ReaderGroupPerformance> mergedReaderGroups(ReaderPerformance worker1, ReaderPerformance worker2) {
        Map<String, ReaderGroupPerformance> mergedStats = new HashMap<>();

        // Merge from Group 1 first

        Set<String> allGroups = new HashSet<>();
        allGroups.addAll(worker1.getReaderGroups().keySet());
        allGroups.addAll(worker2.getReaderGroups().keySet());

        for (String groupName : allGroups) {
            ReaderGroupPerformance fromLeftStates = worker1.getReaderGroups().getOrDefault(groupName, new ReaderGroupPerformance());
            ReaderGroupPerformance fromRightStates = worker2.getReaderGroups().getOrDefault(groupName, new ReaderGroupPerformance());

            ReaderGroupPerformance merged = mergeReaderGroup(fromLeftStates, fromRightStates);
            mergedStats.put(groupName, merged);
        }

        return mergedStats;
    }

    private void updateRuntimeTimes(TestRuntime runtime) {
        runtime.setMinutesRunning(TimeUnit.MILLISECONDS.toMinutes(runningMillis));
        runtime.setHumanRunning(PerformanceUtils.toHumanDuration(runningMillis));

        // If this isn't a "forever" test
        if (runtimeMillis > 0) {
            runtime.setMinutesLeft(TimeUnit.MILLISECONDS.toMinutes(runtimeMillis - runningMillis));
            runtime.setHumanLeft(PerformanceUtils.toHumanDuration(runtimeMillis - runningMillis));
        }
    }

    private void syncStartTime(long startTimeMillis) {
        if (this.startTimeMillis == 0) {
            this.startTimeMillis = startTimeMillis;
        }
        else {
            this.startTimeMillis = Math.min(this.startTimeMillis, startTimeMillis);
        }
    }

    private void syncCurrentTime(long currentTimeMillis) {
        this.currentTimeMillis = Math.max(this.currentTimeMillis, currentTimeMillis);

        if (this.startTimeMillis > 0) {
            runningMillis = this.currentTimeMillis - this.startTimeMillis;
            runningSeconds = TimeUnit.MILLISECONDS.toSeconds(runningMillis);
        }
    }
}
