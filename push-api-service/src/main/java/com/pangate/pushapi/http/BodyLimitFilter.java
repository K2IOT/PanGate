package com.pangate.pushapi.http;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Request bodies above the limit (128 KB, 1 MB for {@code /v1/push:batch}; push-api spec §2.2, §2.4) are refused with
 * 413: at once when Content-Length says so, otherwise (chunked) when the first byte past the limit is read;
 * {@link ProblemHandler} maps {@link BodyTooLargeException} to 413.
 */
public class BodyLimitFilter extends OncePerRequestFilter {

  private final long maxBody;
  private final long maxBatchBody;

  public BodyLimitFilter(long maxBody, long maxBatchBody) {
    this.maxBody = maxBody;
    this.maxBatchBody = maxBatchBody;
  }

  /** Raised while reading a body without Content-Length that grew past the limit. */
  public static final class BodyTooLargeException extends IOException {
    BodyTooLargeException() {
      super("request body too large");
    }
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    long limit = req.getRequestURI().endsWith("/v1/push:batch") ? maxBatchBody : maxBody;
    long length = req.getContentLengthLong();
    if (length > limit) {
      Problems.write(res, 413, "payload_too_large", "request body exceeds " + limit + " bytes");
      return;
    }
    chain.doFilter(length < 0 ? new Limited(req, limit) : req, res);
  }

  private static final class Limited extends HttpServletRequestWrapper {
    private ServletInputStream stream;
    private final long limit;

    Limited(HttpServletRequest req, long limit) {
      super(req);
      this.limit = limit;
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
      if (stream == null) {
        ServletInputStream in = super.getInputStream();
        stream = new ServletInputStream() {
          long read;

          private int count(int n) throws IOException {
            if (n > 0) {
              read += n;
              if (read > limit) {
                throw new BodyTooLargeException();
              }
            }
            return n;
          }

          @Override
          public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
              count(1);
            }
            return b;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            return count(in.read(b, off, len));
          }

          @Override
          public boolean isFinished() {
            return in.isFinished();
          }

          @Override
          public boolean isReady() {
            return in.isReady();
          }

          @Override
          public void setReadListener(ReadListener l) {
            in.setReadListener(l);
          }
        };
      }
      return stream;
    }
  }
}
