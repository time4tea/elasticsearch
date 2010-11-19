package org.elasticsearch.river.couchdb.async;

import java.util.concurrent.TimeUnit;

public class Timeout {
    private long timeoutDurationMs;

    private Timeout(long duration, TimeUnit unit) {
        setTimeoutDuration(duration, unit);
    }

    public void setTimeoutDurationMs(long timeoutDurationMs) {
        this.timeoutDurationMs = timeoutDurationMs;
    }

    public void setTimeoutDuration(long duration, TimeUnit unit) {
        setTimeoutDurationMs(unit.toMillis(duration));
    }

    public TimeoutTimer timer() {
        return new TimeoutTimer(timeoutDurationMs);
    }

    @Override
    public String toString() {
        return timeoutDurationMs + " ms";
    }

    public static Timeout milliseconds(long ms) {
        return new Timeout(ms, TimeUnit.MILLISECONDS);
    }

    public static Timeout seconds(int seconds) {
        return new Timeout(seconds, TimeUnit.SECONDS);
    }

    public static Timeout aMinute() {
        return new Timeout(1, TimeUnit.MINUTES);
    }

    public static Timeout minutes(int minutes) {
        return new Timeout(minutes, TimeUnit.MINUTES);
    }
}
