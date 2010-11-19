package org.elasticsearch.river.couchdb.async;

import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.StringDescription.*;

public class TimeoutTimer {
    private final long startTime = System.currentTimeMillis();
    private final long maxWaitTimeMs;

    public TimeoutTimer(long maxWaitTimeMs) {
        this.maxWaitTimeMs = maxWaitTimeMs;
    }

    public boolean hasTimedOut() {
        return (startTime + maxWaitTimeMs) < System.currentTimeMillis();
    }

    @Override public String toString() {
        return "TimeoutTimer{" +
                "startTime=" + startTime +
                ", maxWaitTimeMs=" + maxWaitTimeMs +
                '}';
    }

    public static TimeoutTimer within(Timeout timeout) {
        return timeout.timer();
    }

    public <T> void assertThat(final String what, final Callable<T> retriever, final Matcher<T> expectedValue) throws Exception {
        assertThat(new Probe() {
            private Object result;

            public void probe() {
                try {
                    result = retriever.call();
                } catch (Exception e) {
                    result = e;
                }
            }

            public boolean isOk() {
                return expectedValue.matches(result);
            }

            public void describeTo(Description description) {
                description.appendText(what)
                        .appendText("\n    expected: ")
                        .appendDescriptionOf(expectedValue)
                        .appendText("\n    got:      ")
                        .appendValue(result);
            }
        });
    }

    public void assertThat(Probe probe) throws Exception {
        while (!hasTimedOut()) {
            probe.probe();

            if (probe.isOk()) {
                return;
            }

            Thread.sleep(50);
        }

        throw new TimeoutException(asString(probe));
    }
}
