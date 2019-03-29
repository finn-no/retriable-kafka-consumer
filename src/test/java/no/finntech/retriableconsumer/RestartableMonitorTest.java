package no.finntech.retriableconsumer;

import org.junit.Test;

import java.util.Arrays;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class RestartableMonitorTest {

  @Test
  public void dont_restart_a_running_restartable() {
    RestartableMonitor monitor = new RestartableMonitor(singletonList(runningRestartable));
    long count = monitor.monitor();
    assertThat(count).isEqualTo(0);
  }

  @Test
  public void restart_nonrunning_restartable() {
    RestartableMonitor monitor = new RestartableMonitor(singletonList(deadRestartable));
    long count = monitor.monitor();
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void start() {
    RestartableMonitor monitor = new RestartableMonitor(singletonList(deadRestartable));
    long count = monitor.start();
    assertThat(count).isEqualTo(1);
  }

  @Test
  public void restart_only_nonrunning_restartables() {
    RestartableMonitor monitor =
        new RestartableMonitor(
            Arrays.asList(deadRestartable, runningRestartable, runningRestartable));
    long restartedCount = monitor.monitor();
    assertThat(restartedCount).isEqualTo(1);
  }

  private final Restartable runningRestartable =
      new Restartable() {
        @Override
        public boolean isRunning() {
          return true;
        }

        @Override
        public String getName() {
          return "";
        }

          @Override
        public void run() {}

        public void close() {}
      };

  private final Restartable deadRestartable =
      new Restartable() {
        @Override
        public boolean isRunning() {
          return false;
        }

        @Override
        public String getName() {
          return "";
        }

          @Override
        public void run() {}

        public void close() {}
      };
}
