package no.finn.retriableconsumer;

import java.io.Closeable;

public interface Restartable extends Runnable, Closeable {

  /**
   * Check if this Runnable is alive. If not, it will be restarted.
   *
   * @return true if it is running, false if not.
   */
  boolean isRunning();

  String getName();

}
