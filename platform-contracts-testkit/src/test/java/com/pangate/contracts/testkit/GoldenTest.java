package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.junit.jupiter.api.Test;

/** Golden files catch any change of encoding: renumbered, retyped or reused fields (Review Focus 3). */
class GoldenTest {

  @Test
  void everyGoldenEncodesToItsStoredBytes() {
    assertThat(Golden.NAMES).hasSize(13);
    for (String n : Golden.NAMES) {
      assertThat(Golden.fixture(n).toByteArray()).as(n).isEqualTo(Golden.bytes(n));
    }
  }

  @Test
  void everyGoldenTextParsesToItsFixture() throws Exception {
    for (String n : Golden.NAMES) {
      Message fixture = Golden.fixture(n);
      Message.Builder b = fixture.newBuilderForType();
      TextFormat.merge(Golden.text(n), b);
      assertThat(b.build()).as(n).isEqualTo(fixture);
    }
  }

  @Test
  void storedBytesStillParse() throws Exception {
    for (String n : Golden.NAMES) {
      Message fixture = Golden.fixture(n);
      assertThat(fixture.getParserForType().parseFrom(Golden.bytes(n))).as(n).isEqualTo(fixture);
    }
  }
}
