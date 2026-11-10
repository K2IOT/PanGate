package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class RegistryValuesTest {

  @Test
  void registryV2RoundTripsADeviceWithPipes() {                          // Review Focus 1
    String v = RegistryValues.v2("gw-1", 7, 1000, "ios", "dev|with|pipes");
    assertThat(v).isEqualTo("gw-1|7|1000|ios|dev|with|pipes");
    assertThat(RegistryValues.parse(v))
        .hasValue(new RegistryValues.Parsed("gw-1", 7, 1000, "ios", Optional.of("dev|with|pipes")));
    assertThat(RegistryValues.v2("gw-1", 7, 1000, null, "d1")).isEqualTo("gw-1|7|1000||d1");
  }

  @Test
  void v1ParsesButNeverMatchesADevice() {                                // Review Focus 2
    var p = RegistryValues.parse("gw-1|7|1000|ios").orElseThrow();
    assertThat(p.device()).isEmpty();
    assertThat(RegistryValues.matchesDevice(p, "d1")).isFalse();
    var emptyDevice = RegistryValues.parse("gw-1|7|1000||").orElseThrow();
    assertThat(RegistryValues.matchesDevice(emptyDevice, "")).isFalse();
    assertThat(RegistryValues.matchesDevice(RegistryValues.parse("gw-1|7|1000|ios|d1").orElseThrow(), "d1")).isTrue();
  }

  @Test
  void malformedValuesAreEmpty() {
    for (String bad : new String[] {"gw-1|7", "gw-1|x|1000|ios", "", "gw-1|7|-1|ios", "|7|1000|ios"}) {
      assertThat(RegistryValues.parse(bad)).as(bad).isEmpty();
    }
  }

  @Test
  void casesAgreeWithTheParser() {
    assertThat(RegistryValues.cases()).hasSizeGreaterThanOrEqualTo(8);
    for (var c : RegistryValues.cases()) {
      assertThat(RegistryValues.parse(c.value())).as(c.name()).isEqualTo(c.expected());
    }
  }

  @Test
  void numbersAreAsciiDigitsOnly() {
    for (String bad : new String[] {"gw-1|+7|1000|ios", "gw-1|7|+1000|ios", "gw-1|\u0667|1000|ios", "gw-1|7|1 0|ios",
        "gw-1|7"}) {
      assertThat(RegistryValues.parse(bad)).as(bad).isEmpty();
    }
    assertThat(RegistryValues.parse("gw|7|1000||d1")).hasValue(new RegistryValues.Parsed("gw", 7, 1000, "",
        Optional.of("d1")));
  }
}
