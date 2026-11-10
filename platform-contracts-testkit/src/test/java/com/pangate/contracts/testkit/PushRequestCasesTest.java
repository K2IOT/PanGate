package com.pangate.contracts.testkit;

import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.PushResponse.Code;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class PushRequestCasesTest {

  private static PushRequestCases.Case named(String name) {
    return PushRequestCases.all().stream().filter(c -> c.name().equals(name)).findFirst()
        .orElseThrow(() -> new AssertionError("no case " + name));
  }

  @Test
  void everyRuleHasAPassingAndAFailingCase() {
    var byRule = PushRequestCases.all().stream().collect(groupingBy(PushRequestCases.Case::rule));
    for (String r : List.of("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V10", "V11", "V12", "V13", "V14")) {
      assertThat(byRule.get(r)).as(r).isNotNull().anyMatch(c -> c.expected() == Code.CODE_OK)
          .anyMatch(c -> c.expected() == Code.CODE_INVALID);
    }
    assertThat(byRule.get("V9")).singleElement().satisfies(c -> {          // Review Focus 4
      assertThat(c.request()).isEqualTo(Fixtures.kickRequest010());
      assertThat(c.expected()).isEqualTo(Code.CODE_OK);
    });
    assertThat(PushRequestCases.all()).extracting(PushRequestCases.Case::name).doesNotHaveDuplicates();
  }

  @Test
  void boundaryCasesUseUtf8Bytes() {
    assertThat(named("V5 principal 256 bytes").request().getPrincipalId().getBytes(StandardCharsets.UTF_8)).hasSize(256);
    var tooLong = named("V5 principal 257 bytes (multi-byte last char)");
    assertThat(tooLong.request().getPrincipalId().getBytes(StandardCharsets.UTF_8)).hasSize(257);
    assertThat(tooLong.request().getPrincipalId()).hasSize(256);
    assertThat(tooLong.expected()).isEqualTo(Code.CODE_INVALID);
    assertThat(named("V6 device 128 bytes").expected()).isEqualTo(Code.CODE_OK);
    assertThat(named("V6 device 129 bytes").expected()).isEqualTo(Code.CODE_INVALID);
    assertThat(named("V6 device with slash").expected()).isEqualTo(Code.CODE_INVALID);
    assertThat(named("V7 payload 65536 bytes").request().getPayload().size()).isEqualTo(65_536);
    assertThat(named("V7 payload 65537 bytes").expected()).isEqualTo(Code.CODE_INVALID);
    assertThat(named("V7 ttl 604800").expected()).isEqualTo(Code.CODE_OK);
    assertThat(named("V7 ttl 604801").expected()).isEqualTo(Code.CODE_INVALID);
  }
}
