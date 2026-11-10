package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.junit.jupiter.api.Test;

/** Golden files catch any change of encoding: renumbered, retyped or reused fields (Review Focus 3). */
class GoldenTest {

  @Test
  void everyGoldenEncodesToItsStoredBytes() {
    assertThat(Golden.NAMES).hasSize(17);
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

  /** Full goldens set every field, so a type change of a field no other fixture sets is still caught. */
  @Test
  void fullGoldensSetEveryField() {
    for (String n : java.util.List.of("push_record_full", "push_request_full", "push_response_full",
        "delivered_record_with_sender")) {
      Message m = Golden.fixture(n);
      var d = m.getDescriptorForType();
      long plain = d.getFields().stream().filter(f -> f.getRealContainingOneof() == null).count();
      assertThat(m.getAllFields()).as(n).hasSize((int) plain + d.getRealOneofs().size());
    }
  }
}
