package com.pangate.gateway.auth;

import com.nimbusds.jose.jwk.JWKSet;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.ParseException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** JWKS over HTTP(S) with ETag revalidation. */
public final class HttpJwksSource implements JwksSource {

  private static final Pattern MAX_AGE = Pattern.compile("max-age\\s*=\\s*(\\d+)");

  private final URI uri;
  private final HttpClient client;
  private final Duration timeout;

  public HttpJwksSource(URI uri, HttpClient client, Duration timeout) {
    this.uri = uri;
    this.client = client;
    this.timeout = timeout;
  }

  @Override
  public FetchResult fetch(String etag) throws IOException {
    HttpRequest.Builder req = HttpRequest.newBuilder(uri).timeout(timeout).GET();
    if (etag != null) {
      req.header("If-None-Match", etag);
    }
    HttpResponse<String> res;
    try {
      res = client.send(req.build(), HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted fetching " + uri, e);
    }
    Duration maxAge = maxAge(res.headers());
    if (res.statusCode() == 304) {
      return new NotModified(maxAge);
    }
    if (res.statusCode() != 200) {
      throw new IOException("JWKS " + uri + " answered HTTP " + res.statusCode());
    }
    try {
      return new Updated(JWKSet.parse(res.body()), res.headers().firstValue("ETag").orElse(null), maxAge);
    } catch (ParseException e) {
      throw new IOException("JWKS " + uri + " is not valid JSON: " + e.getMessage(), e);
    }
  }

  private static Duration maxAge(HttpHeaders headers) {
    return headers.firstValue("Cache-Control").map(MAX_AGE::matcher).filter(Matcher::find)
        .map(m -> Duration.ofSeconds(Long.parseLong(m.group(1)))).orElse(null);
  }

  @Override
  public String toString() {
    return uri.toString();
  }
}
