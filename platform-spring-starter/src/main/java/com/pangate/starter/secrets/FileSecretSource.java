package com.pangate.starter.secrets;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.io.UncheckedIOException;

/** Secrets stored as files below a directory; the secret name is a relative path. */
public final class FileSecretSource implements SecretSource {

  private final Path dir;

  public FileSecretSource(Path dir) {
    this.dir = dir.toAbsolutePath().normalize();
  }

  @Override
  public byte[] get(String name) {
    Path file = resolve(name);
    try {
      return Files.readAllBytes(file);
    } catch (NoSuchFileException e) {
      throw new SecretNotFoundException(name, file.toString(), e);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read secret '" + name + "' at " + file, e);
    }
  }

  @Override
  public String describe(String name) {
    return resolve(name).toString();
  }

  private Path resolve(String name) {
    Path file = dir.resolve(name).normalize();
    if (!file.startsWith(dir)) {
      throw new IllegalArgumentException("Secret name escapes the secret directory: " + name);
    }
    return file;
  }
}
