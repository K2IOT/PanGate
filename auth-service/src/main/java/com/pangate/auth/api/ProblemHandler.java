package com.pangate.auth.api;

import com.pangate.auth.domain.DomainException;
import com.pangate.auth.application.NoSigningKeyException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.transaction.TransactionException;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Maps errors of every non-OAuth endpoint to problem+json; never exposes stack traces (spec §6). */
@RestControllerAdvice
public class ProblemHandler {

  @ExceptionHandler(DomainException.class)
  ResponseEntity<ProblemDetail> domain(DomainException e) {
    var b = ResponseEntity.status(e.httpStatus());
    if (e.httpStatus() == 401) {
      b.header(HttpHeaders.WWW_AUTHENTICATE, "Bearer error=\"invalid_token\"");
    }
    return b.body(Problems.of(e.httpStatus(), e.code(), e.getMessage()));
  }

  @ExceptionHandler({HttpMessageNotReadableException.class, HttpMediaTypeNotSupportedException.class})
  ResponseEntity<ProblemDetail> malformed(Exception e) {
    return ResponseEntity.badRequest().body(Problems.of(400, "malformed_request", "request body is not valid JSON"));
  }

  /** PostgreSQL unavailable (spec §6): retry soon; JWKS never gets here. */
  @ExceptionHandler({DataAccessException.class, TransactionException.class})
  ResponseEntity<ProblemDetail> database(Exception e) {
    return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "5")
        .body(Problems.of(503, "db_unavailable", "storage temporarily unavailable"));
  }

  @ExceptionHandler(NoSigningKeyException.class)
  ResponseEntity<ProblemDetail> noKey(NoSigningKeyException e) {
    return ResponseEntity.status(503).header(HttpHeaders.RETRY_AFTER, "5")
        .body(Problems.of(503, "no_signing_key", "no signing key available"));
  }

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  ResponseEntity<ProblemDetail> method(HttpRequestMethodNotSupportedException e) {
    return ResponseEntity.status(405).body(Problems.of(405, "method_not_allowed", e.getMessage()));
  }

  @ExceptionHandler(NoResourceFoundException.class)
  ResponseEntity<ProblemDetail> notFound(NoResourceFoundException e) {
    return ResponseEntity.status(404).body(Problems.of(404, "not_found", "no such endpoint"));
  }
}
