package com.pangate.gateway.auth;

import com.nimbusds.jose.jwk.JWKSet;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;

/** JWKS from a local file (profile local); the etag is the file's modification time. */
public final class FileJwksSource implements JwksSource {

  private final Path file;

  public FileJwksSource(Path file) {
    this.file = file;
  }

  @Override
  public FetchResult fetch(String etag) throws IOException {
    String stamp = Long.toString(Files.getLastModifiedTime(file).toMillis());
    if (stamp.equals(etag)) {
      return new NotModified(null);
    }
    try {
      return new Updated(JWKSet.parse(Files.readString(file, StandardCharsets.UTF_8)), stamp, null);
    } catch (ParseException e) {
      throw new IOException("JWKS file " + file + " is not valid JSON: " + e.getMessage(), e);
    }
  }

  @Override
  public Duration fixedInterval() {
    return Duration.ofSeconds(30);
  }

  @Override
  public String toString() {
    return file.toString();
  }
}
