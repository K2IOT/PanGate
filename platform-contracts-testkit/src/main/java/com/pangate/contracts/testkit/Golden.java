package com.pangate.contracts.testkit;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Golden encodings of the push-path messages ({@code golden/v1/<name>.bin} and {@code .textproto} on the classpath).
 * A test that compares {@link #fixture} with {@link #bytes} fails on any change of encoding.
 */
public final class Golden {

  private static final Map<String, Supplier<Message>> FIXTURES = new LinkedHashMap<>();

  static {
    FIXTURES.put("push_record_message", () -> Fixtures.message("t1:u1"));
    FIXTURES.put("push_record_device", () -> Fixtures.deviceMessage("t1:u1", "d1"));
    FIXTURES.put("push_record_ephemeral", () -> Fixtures.ephemeral("t1:u1"));
    FIXTURES.put("push_record_broadcast", () -> Fixtures.broadcast("t1:news"));
    FIXTURES.put("push_record_control_subscribe", () -> Fixtures.control("t1:u1", Fixtures.subscribe("t1:news")));
    FIXTURES.put("push_record_control_kick", () -> Fixtures.control("t1:u1",
        Fixtures.kick("01JCNEWCONN0000000000000000", Fixtures.FIXED_NOW_MS)));
    FIXTURES.put("offline_record", () -> Fixtures.offline(Fixtures.message("t1:u1"),
        OfflineRecord.Reason.REASON_NACK_TIMEOUT, 3));
    FIXTURES.put("delivered_record", () -> Fixtures.delivered(Fixtures.deviceMessage("t1:u1", "d1"), "01JCCONN"));
    FIXTURES.put("connection_event_node_down", () -> Fixtures.nodeDown("gw-1", 7));
    FIXTURES.put("push_request_message", () -> Fixtures.request(PushRequest.Kind.KIND_MESSAGE));
    FIXTURES.put("push_request_broadcast", () -> Fixtures.request(PushRequest.Kind.KIND_BROADCAST));
    FIXTURES.put("push_request_control_kick_010", Fixtures::kickRequest010);
    FIXTURES.put("push_response_ok", Fixtures::okResponse);
    // Encoding-only goldens that set every field (not valid records), so a type change of any field is caught.
    FIXTURES.put("push_record_full", Golden::fullRecord);
    FIXTURES.put("push_request_full", Golden::fullRequest);
    FIXTURES.put("push_response_full", Golden::fullResponse);
    FIXTURES.put("delivered_record_with_sender", () -> Fixtures.delivered(fullRecord().toBuilder()
        .setDevice(com.pangate.contracts.v1.DeviceRef.newBuilder().setPrincipalId("t1:u1").setDeviceId("d1")).build(),
        "01JCCONN"));
  }

  static com.pangate.contracts.v1.PushRecord fullRecord() {
    return Fixtures.message("t1:u1").toBuilder().setPriority(com.pangate.contracts.v1.PushItem.Priority.PRIORITY_CRITICAL)
        .setCollapseKey("order-42").setReceipt(true).setSenderPrincipalId("t1:sender")
        .setControl(Fixtures.subscribe("t1:news")).build();
  }

  static PushRequest fullRequest() {
    return Fixtures.request(PushRequest.Kind.KIND_MESSAGE).toBuilder().setControl(Fixtures.subscribe("t1:news"))
        .setDeviceId("d1").setTopic("t1:news").setPriority(com.pangate.contracts.v1.PushItem.Priority.PRIORITY_BULK)
        .setCollapseKey("order-42").setReceipt(true).setIdempotencyKey("idem-1").setSenderPrincipalId("t1:sender")
        .build();
  }

  static com.pangate.contracts.v1.PushResponse fullResponse() {
    return Fixtures.okResponse().toBuilder().setError("detail").setTopicSeq(9).setDuplicate(true)
        .setRetryAfterMs(250).build();
  }

  public static final List<String> NAMES = List.copyOf(FIXTURES.keySet());

  private Golden() {}

  public static Message fixture(String name) {
    Supplier<Message> s = FIXTURES.get(name);
    if (s == null) {
      throw new IllegalArgumentException("no golden " + name);
    }
    return s.get();
  }

  public static byte[] bytes(String name) {
    return read(name + ".bin");
  }

  public static String text(String name) {
    return new String(read(name + ".textproto"), StandardCharsets.UTF_8);
  }

  private static byte[] read(String file) {
    try (InputStream in = Golden.class.getResourceAsStream("/golden/v1/" + file)) {
      if (in == null) {
        throw new IllegalStateException("golden file missing on the classpath: golden/v1/" + file);
      }
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Rewrites every golden pair into {@code args[0]} (normally src/main/resources/golden/v1). Only for a deliberate
   * additive change that adds a golden — never to make a red golden test green.
   */
  public static void main(String[] args) throws IOException {
    Path dir = Path.of(args.length > 0 ? args[0] : "src/main/resources/golden/v1");
    Files.createDirectories(dir);
    for (String n : NAMES) {
      Message m = fixture(n);
      Files.write(dir.resolve(n + ".bin"), m.toByteArray());
      Files.writeString(dir.resolve(n + ".textproto"), "# " + m.getDescriptorForType().getFullName()
          + " — generated by Golden.main from Fixtures\n" + TextFormat.printer().printToString(m));
    }
    System.out.println("wrote " + NAMES.size() + " golden pairs to " + dir.toAbsolutePath());
  }
}
