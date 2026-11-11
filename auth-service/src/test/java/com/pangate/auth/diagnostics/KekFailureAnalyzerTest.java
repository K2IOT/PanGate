package com.pangate.auth.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.auth.infrastructure.crypto.Kek;
import com.pangate.starter.secrets.FileSecretSource;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KekFailureAnalyzerTest {

  private final KekFailureAnalyzer analyzer = new KekFailureAnalyzer();

  private static Throwable failure(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      return new IllegalStateException("context failed", e);
    }
    throw new AssertionError("expected a failure");
  }

  @Test
  void kekAnalyzerNamesSecretAndLocation(@TempDir Path dir) {
    var secrets = new FileSecretSource(dir);
    var a = analyzer.analyze(failure(() -> Kek.load(secrets, "v1")));
    assertThat(a).isNotNull();
    assertThat(a.getDescription()).contains("auth.kek.v1").contains(secrets.describe("auth.kek.v1"));
    assertThat(a.getAction()).contains("32");
  }

  @Test
  void wrongLengthIsExplainedToo(@TempDir Path dir) throws Exception {
    Files.write(dir.resolve("auth.kek.v1"), new byte[7]);
    var a = analyzer.analyze(failure(() -> Kek.load(new FileSecretSource(dir), "v1")));
    assertThat(a).isNotNull();
    assertThat(a.getDescription()).contains("auth.kek.v1").contains("7");
  }

  @Test
  void otherFailuresAreNotClaimed() {
    assertThat(analyzer.analyze(new IllegalStateException("x"))).isNull();
  }
}
