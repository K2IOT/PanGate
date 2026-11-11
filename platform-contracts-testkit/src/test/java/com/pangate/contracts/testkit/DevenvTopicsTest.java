package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** The devenv's kafka-init creates every push-path topic with the spec's partitions (push contracts spec §2). */
class DevenvTopicsTest {

  @Test
  void composeCreatesEveryPushTopic() throws Exception {
    String compose = Files.readString(Path.of("../platform-devenv/docker-compose.yml"));
    for (PushTopics.Spec s : PushTopics.all("c1")) {
      var m = Pattern.compile("--topic " + Pattern.quote(s.name()) + " --partitions (\\d+)").matcher(compose);
      assertThat(m.find()).as(s.name() + " created by kafka-init").isTrue();
      assertThat(Integer.parseInt(m.group(1))).as(s.name() + " partitions").isEqualTo(s.partitions());
    }
  }
}
