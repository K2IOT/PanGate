package com.pangate.gateway.auth;

/** Outcome of verifying a resume ticket. */
public sealed interface TicketVerifyResult {

  record Valid(ResumeTicket ticket) implements TicketVerifyResult {}

  record Expired(ResumeTicket ticket) implements TicketVerifyResult {}

  record Invalid(String reason) implements TicketVerifyResult {}
}
