package com.pangate.account.infrastructure.postgres;

import com.pangate.account.application.port.StoreUnavailableException;
import java.util.function.Supplier;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;

/** Turns "PostgreSQL did not answer" into {@link StoreUnavailableException}; other errors pass through. */
final class Db {

  private Db() {}

  static <T> T call(Supplier<T> work) {
    try {
      return work.get();
    } catch (DataAccessResourceFailureException | TransientDataAccessException e) {   // incl. no connection
      throw new StoreUnavailableException(e);
    }
  }

  static void run(Runnable work) {
    call(() -> {
      work.run();
      return null;
    });
  }
}
