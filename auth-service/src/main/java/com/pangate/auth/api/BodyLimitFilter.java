package com.pangate.auth.api;

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
 * Request bodies above 16 KB are refused with 413 (spec §4): at once when Content-Length says so, otherwise (chunked)
 * when the 16 385th byte is read; {@link ProblemHandler} maps {@link BodyTooLargeException} to 413.
 */
public class BodyLimitFilter extends OncePerRequestFilter {

  public static final long MAX_BODY = 16 * 1024;

  /** Raised while reading a body without Content-Length that grew past {@link #MAX_BODY}. */
  public static final class BodyTooLargeException extends IOException {
    BodyTooLargeException() {
      super("request body exceeds 16 KB");
    }
  }

  @Override
  protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws ServletException, IOException {
    long length = req.getContentLengthLong();
    if (length > MAX_BODY) {
      Problems.write(res, 413, "payload_too_large", "request body exceeds 16 KB");
      return;
    }
    chain.doFilter(length < 0 ? new Limited(req) : req, res);
  }

  private static final class Limited extends HttpServletRequestWrapper {
    private ServletInputStream stream;

    Limited(HttpServletRequest req) {
      super(req);
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
              if (read > MAX_BODY) {
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
