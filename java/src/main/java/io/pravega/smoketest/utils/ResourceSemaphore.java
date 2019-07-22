package io.pravega.smoketest.utils;

import java.util.concurrent.Semaphore;

/**
 * An extension of a semaphore. This implementation allows one to {@link #reducePermits()} without blocking like {@link #acquire()}.
 * This behaviour is useful when something wants to notify the world of a decrease in a resource but does not need to block (i.e. they take
 * it unconditionally).
 */
public class ResourceSemaphore extends Semaphore {
    public ResourceSemaphore(int permits) {
        super(permits);
    }

    public void reducePermits() {
        super.reducePermits(1);
    }
    
    public boolean hasAvailablePermits() {
        return super.availablePermits() > 0;
    }
}
