package com.pangate.account;

import static com.pangate.account.support.Http.unique;
import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.config.AdminConnector;
import com.pangate.account.support.AccountIT;
import com.pangate.account.support.Accounts;
import com.pangate.account.support.Accounts.Device;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * Review round 2 (declined to judge, ruled a finding): dot segments in the raw request path must not reach the admin
 * API past the port routing or the admin token check. Sent over a raw socket so no client normalises the path.
 */
@AccountIT
class DotSegmentIT {

  @LocalServerPort int port;
  @Autowired AdminConnector admin;

  static int status(int port, String rawPath, String bearer) throws IOException {
    try (Socket s = new Socket("127.0.0.1", port)) {
      OutputStream out = s.getOutputStream();
      out.write(("GET " + rawPath + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
          + (bearer == null ? "" : "Authorization: Bearer " + bearer + "\r\n") + "\r\n")
          .getBytes(StandardCharsets.US_ASCII));
      out.flush();
      InputStream in = s.getInputStream();
      String head = new String(in.readNBytes(12), StandardCharsets.US_ASCII);
      return Integer.parseInt(head.substring(9, 12));
    }
  }

  @Test
  void dotSegmentsDoNotReachTheAdminApi() throws IOException {
    Device d = Accounts.register(port, unique("dot"));
    String target = "admin/v1/accounts/" + d.userId();
    String user = d.connectToken(port);
    for (String p : new String[] {"/actuator/../" + target, "/actuator/%2e%2e/" + target,
        "/actuator/..;/" + target, "/live/../" + target}) {
      assertThat(status(admin.port(), p, null)).as("admin port " + p).isIn(400, 401, 404);
    }
    for (String p : new String[] {"/v1/../" + target, "/v1/%2e%2e/" + target, "/v1/..;/" + target}) {
      assertThat(status(port, p, user)).as("public port " + p).isIn(400, 401, 404);
    }
  }
}
