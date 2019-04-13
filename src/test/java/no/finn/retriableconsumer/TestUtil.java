package no.finn.retriableconsumer;


import io.vavr.collection.List;

public class TestUtil {
    static final long INTERVAL_MILLIS = 100;

    private static class AssertionErrorContainer {
        private AssertionError assertionError;

        public AssertionError getAssertionError() {
            return assertionError;
        }

        public void setAssertionError(AssertionError assertionError) {
            this.assertionError = assertionError;
        }
    }

    public static void assertDelayed(Runnable assertion, long delayMillis) {
        sleep(delayMillis);
        assertion.run();
    }

    public static void assertWithinTimespan(Runnable assertion, long timeoutMillis) {
        final AssertionErrorContainer assertionErrorContainer = new AssertionErrorContainer();
        List.range(0, numTries(timeoutMillis)).find(i -> {
            try {
                assertion.run();
                return true;
            } catch (AssertionError e) {
                sleep(INTERVAL_MILLIS);
                assertionErrorContainer.setAssertionError(e);
                return false;
            }
        }).onEmpty(() -> { throw assertionErrorContainer.getAssertionError(); });
    }

    private static void sleep(long delayMillis) {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static long numTries(long timeoutMillis) {
        if (timeoutMillis == 0) {
            return 1;
        }

        long numTries;
        numTries = timeoutMillis / INTERVAL_MILLIS;
        return numTries < 2 ? 2 : numTries;
    }
}
