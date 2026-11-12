package com.pangate.chat.http;

import com.pangate.chat.core.StoreUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Errors of the HTTP API as {@code application/problem+json} (chat spec §7, §8). */
@RestControllerAdvice
public class ProblemHandler {

  private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

  @ExceptionHandler(ChatHttpException.class)
  ResponseEntity<ProblemDetail> refused(ChatHttpException e) {
    ResponseEntity.BodyBuilder b = ResponseEntity.status(e.status());
    if (e.status() == 429) {
      b.header(HttpHeaders.RETRY_AFTER, "1");
    }
    return b.body(Problems.of(e.status(), e.code(), e.getMessage()));
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  ProblemDetail unreadable(HttpMessageNotReadableException e) {
    return Problems.of(400, "invalid_request", "the body must be a JSON object");
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  ProblemDetail mediaType(HttpMediaTypeNotSupportedException e) {
    return Problems.of(415, "invalid_request", "Content-Type must be application/json");
  }

  /** ScyllaDB or Kafka did not answer (chat spec §8): 503 with Retry-After. */
  @ExceptionHandler({StoreUnavailableException.class, IllegalStateException.class})
  ResponseEntity<ProblemDetail> unavailable(RuntimeException e) {
    log.warn("unavailable: {}", e.toString());
    return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "1")
        .body(Problems.of(503, "unavailable", "try again shortly"));
  }
}
