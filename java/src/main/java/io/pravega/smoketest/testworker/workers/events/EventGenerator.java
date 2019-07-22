package io.pravega.smoketest.testworker.workers.events;


public interface EventGenerator {
    GeneratedEvent nextEvent();
}
