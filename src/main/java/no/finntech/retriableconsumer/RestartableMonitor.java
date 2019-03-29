package no.finntech.retriableconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public final class RestartableMonitor implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestartableMonitor.class);
  private final List<? extends Restartable> consumers;

  public RestartableMonitor(List<Restartable> consumers) {
    this.consumers = Collections.unmodifiableList(consumers);
  }

  /**
   * Monitors all restartables and restarts them in a new Thread if not running. Returns a count of
   * restarted restartables.
   *
   * @return count of restarted restartables.
   */
  public long monitor() {
    return consumers.stream().map(this::check).filter(restarted -> restarted).count();
  }

  public long start() {
    return consumers.stream().map(this::start).filter(restarted -> restarted).count();
  }

  public boolean start(Restartable restartable) {
    return check(restartable, true);
  }

  private boolean check(Restartable restartable) {
    return check(restartable, false);
  }

  private boolean check(Restartable restartable, boolean start) {
    if (!restartable.isRunning()) {
      if (start) {
        LOGGER.info("Starting monitored restartable [{}].", restartable.getName());
      } else {
        LOGGER.error(
            "Found non-running restartable {}. Will attempt restart.. ", restartable.getName());
      }
      Thread restartedThread = new Thread(restartable, restartable.getName());
      restartedThread.setDaemon(false);
      restartedThread.start();
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    consumers.forEach(
        c -> {
          try {
            c.close();
          } catch (IOException e) {
            LOGGER.error("Error when closing restartable: ", e);
          }
        });
  }

    public int size() {
        return consumers.size();
    }
}
