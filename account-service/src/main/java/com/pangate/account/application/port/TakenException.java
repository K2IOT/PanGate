package com.pangate.account.application.port;

import com.pangate.account.domain.DomainException;

/** A unique username, email or phone is already used: {@code username_taken}, {@code email_taken}, {@code phone_taken}. */
public final class TakenException extends DomainException {

  public TakenException(String code) {
    super(code, code.replace('_', ' '));
  }
}
