package com.pangate.auth.application.port;

/** Runs work in one database transaction. */
@FunctionalInterface
public interface Transactions {
  void inTx(Runnable work);
}
