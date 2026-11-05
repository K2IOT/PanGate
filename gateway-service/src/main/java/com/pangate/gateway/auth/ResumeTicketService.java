package com.pangate.gateway.auth;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Issues and verifies resume tickets offline (spec §4.7). Layout:
 * {@code ver(1)=1 | kid(4) | cell | tenant | principal | device | authAt(8) | exp(8) | jtiHash(8) |
 * count(1) + patterns | hmac(32)}; strings are uint16 length + UTF-8; base64url without padding.
 */
public final class ResumeTicketService {

  private static final byte VERSION = 1;
  private static final int MAC_LEN = 32;
  private static final int MAX_TOKEN_BYTES = 1536;

  // Swapped when the key files rotate (spec §4.7, every 60 s); each issue/verify reads it once.
  private volatile TicketKeyRing ring;

  public ResumeTicketService(TicketKeyRing ring) {
    this.ring = ring;
  }

  public TicketKeyRing ring() {
    return ring;
  }

  public void ring(TicketKeyRing next) {
    this.ring = next;
  }

  public String issue(ResumeTicket t) {
    TicketKey key = ring.current();
    ByteBuffer buf = ByteBuffer.allocate(MAX_TOKEN_BYTES);
    buf.put(VERSION).putInt(key.kid());
    putString(buf, t.cell());
    putString(buf, t.tenant());
    putString(buf, t.principal());
    putString(buf, t.device());
    buf.putLong(t.authAtSec()).putLong(t.expSec()).putLong(t.authJtiHash());
    buf.put((byte) t.topicPatterns().size());
    for (String p : t.topicPatterns()) {
      putString(buf, p);
    }
    int bodyLen = buf.position();
    buf.put(mac(key, buf.array(), bodyLen));
    return Base64.getUrlEncoder().withoutPadding().encodeToString(java.util.Arrays.copyOf(buf.array(), buf.position()));
  }

  public TicketVerifyResult verify(String token, long nowSec) {
    byte[] raw;
    try {
      raw = Base64.getUrlDecoder().decode(token);
    } catch (IllegalArgumentException e) {
      return new TicketVerifyResult.Invalid("not base64url");
    }
    if (raw.length < 1 + 4 + MAC_LEN || raw.length > MAX_TOKEN_BYTES) {
      return new TicketVerifyResult.Invalid("bad length");
    }
    int bodyLen = raw.length - MAC_LEN;
    ByteBuffer buf = ByteBuffer.wrap(raw, 0, bodyLen);
    try {
      if (buf.get() != VERSION) {
        return new TicketVerifyResult.Invalid("unknown version");
      }
      Optional<TicketKey> key = ring.byKid(buf.getInt());
      if (key.isEmpty()) {
        return new TicketVerifyResult.Invalid("unknown kid");
      }
      byte[] expected = mac(key.get(), raw, bodyLen);
      byte[] actual = java.util.Arrays.copyOfRange(raw, bodyLen, raw.length);
      if (!MessageDigest.isEqual(expected, actual)) {
        return new TicketVerifyResult.Invalid("bad signature");
      }
      String cell = getString(buf);
      String tenant = getString(buf);
      String principal = getString(buf);
      String device = getString(buf);
      long authAt = buf.getLong();
      long exp = buf.getLong();
      long jti = buf.getLong();
      int count = buf.get() & 0xFF;
      List<String> patterns = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        patterns.add(getString(buf));
      }
      if (buf.hasRemaining()) {
        return new TicketVerifyResult.Invalid("trailing bytes");
      }
      ResumeTicket t = new ResumeTicket(cell, tenant, principal, device, authAt, exp, jti, patterns);
      return nowSec >= exp ? new TicketVerifyResult.Expired(t) : new TicketVerifyResult.Valid(t);
    } catch (BufferUnderflowException | IllegalArgumentException e) {
      return new TicketVerifyResult.Invalid("malformed");
    }
  }

  /** First 8 bytes of SHA-256(jti), big-endian: lets a ticket be revoked by the JWT id it came from. */
  public static long jtiHash(String jti) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-256").digest(jti.getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(d, 0, 8).getLong();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] mac(TicketKey key, byte[] data, int len) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key.key(), "HmacSHA256"));
      mac.update(data, 0, len);
      return mac.doFinal();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void putString(ByteBuffer buf, String s) {
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    if (b.length > 0xFFFF) {
      throw new IllegalArgumentException("string too long");
    }
    buf.putShort((short) b.length).put(b);
  }

  private static String getString(ByteBuffer buf) {
    int len = buf.getShort() & 0xFFFF;
    if (len > buf.remaining()) {
      throw new BufferUnderflowException();
    }
    byte[] b = new byte[len];
    buf.get(b);
    return new String(b, StandardCharsets.UTF_8);
  }
}
