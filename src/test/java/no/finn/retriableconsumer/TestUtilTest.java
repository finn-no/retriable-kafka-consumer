package no.finn.retriableconsumer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUtilTest {
    class TestAssertion implements Runnable {
        private final int numInitialFails;

        private int executions = 0;

        public TestAssertion() {
            this(0);
        }

        public TestAssertion(int numInitialFails) {
            this.numInitialFails = numInitialFails;
        }

        @Override
        public void run() {
            executions++;
            if (executions <= numInitialFails) {
                throw new AssertionError("Deliberate test assertion failure");
            }
        }

        public int getExecutions() {
            return executions;
        }

    }

    @Test
    public void runs_once_with_failure_on_first_and_zero_timeout() {
        final TestAssertion testAssertion = new TestAssertion(1);

        try {
            TestUtil.assertWithinTimespan(testAssertion, 0);
            fail("Should have throw AssertionError");
        } catch (AssertionError e) {
            assertSame(1, testAssertion.getExecutions());
        }
    }

    @Test
    public void runs_once_when_successful_first_execution() {
        final TestAssertion testAssertion = new TestAssertion();

        TestUtil.assertWithinTimespan(testAssertion, 500);
        assertSame(1, testAssertion.getExecutions());
    }

    @Test
    public void one_retry_when_failure_first_execution_with_minimal_timeout() {
        final TestAssertion testAssertion = new TestAssertion(1);

        TestUtil.assertWithinTimespan(testAssertion, 1);
        assertSame(2, testAssertion.getExecutions());
    }

    @Test
    public void runs_one_successful_time_after_x_fails() {
        final int numAssertionFails = 3;
        final TestAssertion testAssertion = new TestAssertion(numAssertionFails);

        TestUtil.assertWithinTimespan(testAssertion, 500);
        assertSame(numAssertionFails + 1, testAssertion.getExecutions());
    }

    @Test
    public void ultimately_fails_on_timeout() {
        final int numAssertionFails = 20;
        final long timeout = 600;
        final TestAssertion testAssertion = new TestAssertion(numAssertionFails);

        try {
            TestUtil.assertWithinTimespan(testAssertion, timeout);
            fail("Should have throw AssertionError");
        } catch (AssertionError e) {
            assertEquals(timeout / TestUtil.INTERVAL_MILLIS, testAssertion.getExecutions());
        }
    }

    private static class InvokeChecker {
        private boolean invoked = false;

        public void invoke() {
            this.invoked = true;
        }

        public boolean isInvoked() {
            return invoked;
        }
    }

    @Test
    public void invokes_only_after_delay_has_passed() {
        final long delay = 1000;
        final InvokeChecker invokeChecker = new InvokeChecker();
        final long start = System.currentTimeMillis();
        new Thread(() -> {
            while (System.currentTimeMillis() < start + delay) {
                assertFalse(invokeChecker.isInvoked());
                sleep(100);
            }
        }).run();

        new Thread(() -> TestUtil.assertDelayed(invokeChecker::invoke, delay))
                .run();

        sleep(delay + 100);
        assertTrue(invokeChecker.isInvoked());
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
