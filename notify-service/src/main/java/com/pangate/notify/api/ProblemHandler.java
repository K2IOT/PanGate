package com.pangate.notify.api;

import com.pangate.notify.application.port.StoreUnavailableException;
import com.pangate.notify.domain.DomainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Errors as {@code application/problem+json} with a stable {@code code} (offline notifications spec §5.1). */
@RestControllerAdvice
public class ProblemHandler {

  private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

  static int status(String code) {
    return switch (code) {
      case "rate_limited" -> 429;
      case "invalid_token" -> 401;
      case "unavailable" -> 503;
      default -> 400;
    };
  }

  @ExceptionHandler(DomainException.class)
  ResponseEntity<ProblemDetail> refused(DomainException e) {
    int status = status(e.code());
    ResponseEntity.BodyBuilder b = ResponseEntity.status(status);
    if (status == 429) {
      b.header(HttpHeaders.RETRY_AFTER, "60");
    }
    return b.contentType(MediaType.APPLICATION_PROBLEM_JSON).body(Problems.of(status, e.code(), e.getMessage()));
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
