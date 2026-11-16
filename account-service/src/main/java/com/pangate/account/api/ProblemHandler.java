package com.pangate.account.api;

import com.pangate.account.application.port.StoreUnavailableException;
import com.pangate.account.domain.DomainException;
import com.pangate.account.domain.RateLimitedException;
import com.pangate.account.infrastructure.crypto.BusyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Errors as {@code application/problem+json} with a stable {@code code} (account spec §4). */
@RestControllerAdvice
public class ProblemHandler {

  private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

  @ExceptionHandler(DomainException.class)
  ResponseEntity<ProblemDetail> refused(DomainException e) {
    int status = Statuses.of(e.code());
    ResponseEntity.BodyBuilder b = ResponseEntity.status(status);
    if (e instanceof RateLimitedException r) {
      b.header(HttpHeaders.RETRY_AFTER, Long.toString(r.retryAfterSec()));
    }
    return b.contentType(org.springframework.http.MediaType.APPLICATION_PROBLEM_JSON)
        .body(Problems.of(status, e.code(), e.getMessage()));
  }

  @ExceptionHandler(BusyException.class)
  ResponseEntity<ProblemDetail> busy(BusyException e) {
    return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1")
        .body(Problems.of(503, "unavailable", "try again shortly"));
  }

  @ExceptionHandler(StoreUnavailableException.class)
  ResponseEntity<ProblemDetail> store(StoreUnavailableException e) {
    log.warn("database unavailable: {}", String.valueOf(e.getCause()));
    return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "5")
        .body(Problems.of(503, "unavailable", "try again shortly"));
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  ProblemDetail unreadable(HttpMessageNotReadableException e) {
    return Problems.of(400, "invalid_request", "the body must be a JSON object");
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  ProblemDetail mediaType(HttpMediaTypeNotSupportedException e) {
    return Problems.of(415, "invalid_request", "Content-Type must be application/json");
  }
}
