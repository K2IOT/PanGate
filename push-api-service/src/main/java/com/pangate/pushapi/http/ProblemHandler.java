package com.pangate.pushapi.http;

import com.pangate.pushapi.core.BadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Errors of the HTTP surface as {@code application/problem+json} with a stable {@code code} (push-api spec §2.4). */
@RestControllerAdvice
public class ProblemHandler {

  private static final Logger log = LoggerFactory.getLogger(ProblemHandler.class);

  @ExceptionHandler(BadRequestException.class)
  ProblemDetail badRequest(BadRequestException e) {
    return Problems.of(400, e.code(), e.getMessage());
  }

  @ExceptionHandler(HttpMessageNotReadableException.class)
  ProblemDetail unreadable(HttpMessageNotReadableException e) {
    if (e.getCause() instanceof BodyLimitFilter.BodyTooLargeException
        || (e.getCause() != null && e.getCause().getCause() instanceof BodyLimitFilter.BodyTooLargeException)) {
      return Problems.of(413, "payload_too_large", "request body too large");
    }
    return Problems.of(400, "invalid_request", "the body must be a JSON object");
  }

  @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
  ProblemDetail mediaType(HttpMediaTypeNotSupportedException e) {
    return Problems.of(415, "unsupported_media_type", "Content-Type must be application/json");
  }

  @ExceptionHandler(BodyLimitFilter.BodyTooLargeException.class)
  ProblemDetail tooLarge(BodyLimitFilter.BodyTooLargeException e) {
    return Problems.of(413, "payload_too_large", "request body too large");
  }

  @ExceptionHandler(IllegalStateException.class)
  ProblemDetail internal(IllegalStateException e) {
    log.error("internal error: {}", e.getMessage());
    return Problems.of(500, "internal", "internal error");
  }
}
