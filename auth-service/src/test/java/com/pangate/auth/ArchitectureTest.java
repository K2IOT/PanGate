package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Hexagonal rule (plan Global Constraints): domain and application import no adapters, config or frameworks. */
class ArchitectureTest {

  static final List<String> FORBIDDEN = List.of("import com.pangate.auth.infrastructure.", "import com.pangate.auth.api.",
      "import com.pangate.auth.config.", "import org.springframework.", "import org.apache.kafka.", "import java.sql.",
      "import com.pangate.contracts.");

  @Test
  void domainAndApplicationStayFreeOfAdaptersAndFrameworks() throws IOException {
    Path root = Path.of("src/main/java/com/pangate/auth");
    try (Stream<Path> files = Stream.concat(Files.walk(root.resolve("domain")), Files.walk(root.resolve("application")))) {
      List<String> violations = files.filter(f -> f.toString().endsWith(".java")).flatMap(f -> {
        try {
          return Files.readAllLines(f).stream().filter(l -> FORBIDDEN.stream().anyMatch(l::startsWith))
              .map(l -> root.relativize(f) + ": " + l);
        } catch (IOException e) {
          throw new java.io.UncheckedIOException(e);
        }
      }).toList();
      assertThat(violations).isEmpty();
    }
  }
}
