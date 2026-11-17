package com.pangate.account.application.port;

import java.util.function.Supplier;

/** One database transaction around a unit of work. */
public interface Transactions {

  <T> T inTx(Supplier<T> work);

  void inTx(Runnable work);
}
