package com.pangate.auth.api;

import com.pangate.auth.domain.DomainException;
import org.springframework.http.HttpHeaders;
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

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  ResponseEntity<ProblemDetail> method(HttpRequestMethodNotSupportedException e) {
    return ResponseEntity.status(405).body(Problems.of(405, "method_not_allowed", e.getMessage()));
  }

  @ExceptionHandler(NoResourceFoundException.class)
  ResponseEntity<ProblemDetail> notFound(NoResourceFoundException e) {
    return ResponseEntity.status(404).body(Problems.of(404, "not_found", "no such endpoint"));
  }
}
