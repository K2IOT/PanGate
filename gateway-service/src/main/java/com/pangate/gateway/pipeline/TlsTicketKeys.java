package com.pangate.gateway.pipeline;

import com.pangate.starter.secrets.SecretSource;
import io.netty.handler.ssl.OpenSslSessionContext;
import io.netty.handler.ssl.OpenSslSessionTicketKey;
import io.netty.handler.ssl.SslContext;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * The cell's TLS session ticket keys (spec §7.7): {@code tls-ticket-keys/{prev,current,next}}, each one base64 line of
 * 48 bytes = name(16) | hmac(16) | aes(16). Shared by every node so a client resumes TLS on any of them; the first
 * key handed to OpenSSL (current) encrypts new tickets, all three decrypt.
 */
public final class TlsTicketKeys {

  private static final String[] ROLES = {"current", "next", "prev"};

  private final byte[][] keys;  // current, next, prev

  private TlsTicketKeys(byte[][] keys) {
    this.keys = keys;
  }

  public static TlsTicketKeys load(SecretSource s) {
    byte[][] keys = new byte[3][];
    for (int i = 0; i < ROLES.length; i++) {
      String name = "tls-ticket-keys/" + ROLES[i];
      byte[] k;
      try {
        k = Base64.getDecoder().decode(new String(s.get(name), StandardCharsets.US_ASCII).trim());
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException("Invalid TLS ticket key " + s.describe(name) + ": not base64", e);
      }
      if (k.length != OpenSslSessionTicketKey.TICKET_KEY_SIZE) {
        throw new IllegalStateException("Invalid TLS ticket key " + s.describe(name) + ": expected "
            + OpenSslSessionTicketKey.TICKET_KEY_SIZE + " bytes, got " + k.length);
      }
      keys[i] = k;
    }
    return new TlsTicketKeys(keys);
  }

  /** Installs the keys in an OpenSSL context; a JDK-provider context (no tcnative) has no ticket key API. */
  public void applyTo(SslContext ctx) {
    if (ctx.sessionContext() instanceof OpenSslSessionContext s) {
      OpenSslSessionTicketKey[] out = new OpenSslSessionTicketKey[keys.length];
      for (int i = 0; i < keys.length; i++) {
        byte[] k = keys[i];
        out[i] = new OpenSslSessionTicketKey(Arrays.copyOfRange(k, 0, 16), Arrays.copyOfRange(k, 16, 32),
            Arrays.copyOfRange(k, 32, 48));
      }
      s.setTicketKeys(out);
    }
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof TlsTicketKeys t && Arrays.deepEquals(keys, t.keys);
  }

  @Override
  public int hashCode() {
    return Arrays.deepHashCode(keys);
  }
}
