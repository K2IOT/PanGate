package com.pangate.contracts.testkit;

import com.google.protobuf.ByteString;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushRequest.Kind;
import com.pangate.contracts.v1.PushResponse.Code;
import java.util.ArrayList;
import java.util.List;

/**
 * Validation cases for {@code PushService.SendStream} (push contracts spec §3.1, rules V1–V9). Push API must answer each
 * request with the expected code (rules V1–V11); this class holds data only, the validator lives in push-api-service.
 */
public final class PushRequestCases {

  /** One case: {@code rule} is "V1" … "V9". */
  public record Case(String name, String rule, PushRequest request, Code expected) {}

  private PushRequestCases() {}

  private static PushRequest.Builder message() {
    return Fixtures.request(Kind.KIND_MESSAGE).toBuilder();
  }

  private static PushRequest.Builder broadcast() {
    return Fixtures.request(Kind.KIND_BROADCAST).toBuilder();
  }

  private static PushRequest.Builder control() {
    return Fixtures.request(Kind.KIND_CONTROL).toBuilder();
  }

  private static ByteString bytes(int n) {
    return ByteString.copyFrom(new byte[n]);
  }

  public static List<Case> all() {
    List<Case> c = new ArrayList<>();
    String ok256 = "t1:" + "a".repeat(253);
    String bad257 = "t1:" + "a".repeat(252) + "é";

    c.add(new Case("V1 message is valid", "V1", message().build(), Code.CODE_OK));
    c.add(new Case("V1 kind unspecified", "V1", message().setKind(Kind.KIND_UNSPECIFIED).build(), Code.CODE_INVALID));

    c.add(new Case("V2 message to a principal", "V2", message().build(), Code.CODE_OK));
    c.add(new Case("V2 message without principal", "V2", message().clearPrincipalId().build(), Code.CODE_INVALID));
    c.add(new Case("V2 message without payload", "V2", message().clearPayload().build(), Code.CODE_INVALID));
    c.add(new Case("V2 message with topic", "V2", message().setTopic("t1:news").build(), Code.CODE_INVALID));
    c.add(new Case("V2 message with control", "V2", message().setControl(Fixtures.subscribe("t1:x")).build(),
        Code.CODE_INVALID));

    c.add(new Case("V3 broadcast", "V3", broadcast().build(), Code.CODE_OK));
    c.add(new Case("V3 broadcast without topic", "V3", broadcast().clearTopic().build(), Code.CODE_INVALID));
    c.add(new Case("V3 broadcast without payload", "V3", broadcast().clearPayload().build(), Code.CODE_INVALID));
    c.add(new Case("V3 broadcast with principal", "V3", broadcast().setPrincipalId("t1:u1").build(), Code.CODE_INVALID));
    c.add(new Case("V3 broadcast with device", "V3", broadcast().setDeviceId("d1").build(), Code.CODE_INVALID));
    c.add(new Case("V3 broadcast with control", "V3", broadcast().setControl(Fixtures.subscribe("t1:x")).build(),
        Code.CODE_INVALID));

    c.add(new Case("V4 control subscribe", "V4", control().build(), Code.CODE_OK));
    c.add(new Case("V4 control without op", "V4", control().setControl(ControlOp.getDefaultInstance()).build(),
        Code.CODE_INVALID));
    c.add(new Case("V4 control without principal", "V4", control().clearPrincipalId().build(), Code.CODE_INVALID));
    c.add(new Case("V4 control with payload", "V4", control().setPayload(Fixtures.PAYLOAD).build(), Code.CODE_INVALID));
    c.add(new Case("V4 control with topic", "V4", control().setTopic("t1:news").build(), Code.CODE_INVALID));

    c.add(new Case("V5 principal 256 bytes", "V5", message().setPrincipalId(ok256).build(), Code.CODE_OK));
    c.add(new Case("V5 principal 257 bytes (multi-byte last char)", "V5", message().setPrincipalId(bad257).build(),
        Code.CODE_INVALID));
    c.add(new Case("V5 principal of another tenant", "V5", message().setPrincipalId("t2:u1").build(),
        Code.CODE_INVALID));
    c.add(new Case("V5 topic of another tenant", "V5", broadcast().setTopic("t2:news").build(), Code.CODE_INVALID));
    c.add(new Case("V5 topic 256 bytes", "V5", broadcast().setTopic(ok256).build(), Code.CODE_OK));
    c.add(new Case("V5 topic 257 bytes", "V5", broadcast().setTopic(bad257).build(), Code.CODE_INVALID));

    c.add(new Case("V6 device 128 bytes", "V6", message().setDeviceId("d".repeat(128)).build(), Code.CODE_OK));
    c.add(new Case("V6 device 129 bytes", "V6", message().setDeviceId("d".repeat(127) + "é").build(),
        Code.CODE_INVALID));
    c.add(new Case("V6 device with slash", "V6", message().setDeviceId("phone/1").build(), Code.CODE_INVALID));

    c.add(new Case("V7 payload 65536 bytes", "V7", message().setPayload(bytes(65_536)).build(), Code.CODE_OK));
    c.add(new Case("V7 payload 65537 bytes", "V7", message().setPayload(bytes(65_537)).build(), Code.CODE_INVALID));
    c.add(new Case("V7 ttl 604800", "V7", message().setTtlSec(604_800).build(), Code.CODE_OK));
    c.add(new Case("V7 ttl 604801", "V7", message().setTtlSec(604_801).build(), Code.CODE_INVALID));

    c.add(new Case("V8 tenant t1", "V8", message().build(), Code.CODE_OK));
    c.add(new Case("V8 empty tenant", "V8", message().setTenantId("").setPrincipalId(":u1").build(),
        Code.CODE_INVALID));
    c.add(new Case("V8 warm-up tenant", "V8", message().setTenantId("__warmup").setPrincipalId("__warmup:u1").build(),
        Code.CODE_INVALID));

    c.add(new Case("V9 gateway 0.1.0 KICK", "V9", Fixtures.kickRequest010(), Code.CODE_OK));

    c.add(new Case("V10 receipt with RELIABLE", "V10", message().setReceipt(true).build(), Code.CODE_OK));
    c.add(new Case("V10 receipt with EPHEMERAL", "V10", message().setReceipt(true)
        .setClass_(com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_EPHEMERAL).build(), Code.CODE_INVALID));
    c.add(new Case("V10 receipt on BROADCAST", "V10", broadcast().setReceipt(true).build(), Code.CODE_INVALID));

    c.add(new Case("V11 device with RELIABLE", "V11", message().setDeviceId("d1").build(), Code.CODE_OK));
    c.add(new Case("V11 device with ORDERED", "V11", message().setDeviceId("d1")
        .setClass_(com.pangate.contracts.v1.DeliveryClass.DELIVERY_CLASS_ORDERED).build(), Code.CODE_INVALID));
    return List.copyOf(c);
  }
}
