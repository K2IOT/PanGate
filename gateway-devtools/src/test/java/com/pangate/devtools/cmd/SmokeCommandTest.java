package com.pangate.devtools.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.devtools.Main;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class SmokeCommandTest {

  @Test
  void routerOptionRequired() {
    StringWriter err = new StringWriter();
    CommandLine cmd = new CommandLine(new Main());
    cmd.setErr(new PrintWriter(err));
    int code = cmd.execute("smoke", "--url", "wss://localhost:1/v1/ws", "--issuer", "http://localhost:1",
        "--insecure");
    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("--router");
  }

  private static int run(StringWriter err, String... args) {
    CommandLine cmd = new CommandLine(new Main());
    cmd.setErr(new PrintWriter(err));
    return cmd.execute(args);
  }

  @Test
  void issuerAndAuthUrlAreMutuallyExclusive() {
    StringWriter err = new StringWriter();
    int code = run(err, "smoke", "--url", "wss://localhost:1/v1/ws", "--router", "r:1", "--insecure",
        "--issuer", "http://localhost:1", "--auth-url", "http://localhost:2", "--client-file", "/x");
    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("mutually exclusive");
  }

  @Test
  void oneTokenSourceIsRequired() {
    StringWriter err = new StringWriter();
    int code = run(err, "smoke", "--url", "wss://localhost:1/v1/ws", "--router", "r:1", "--insecure");
    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("--issuer").contains("--auth-url");
  }

  @Test
  void authUrlNeedsAClientFile() {
    StringWriter err = new StringWriter();
    int code = run(err, "smoke", "--url", "wss://localhost:1/v1/ws", "--router", "r:1", "--insecure",
        "--auth-url", "http://localhost:2");
    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("--client-file");
  }

  @Test
  void tokenCommandNeedsAKeyOrAnAuthUrl() {
    StringWriter err = new StringWriter();
    int code = run(err, "token", "--tenant", "t1", "--principal", "t1:u", "--device", "d");
    assertThat(code).isEqualTo(2);
    assertThat(err.toString()).contains("--key").contains("--auth-url");
  }

  @Test
  void authRunsUseAFreshPrincipalBecauseTheRevokeStepLastsADay() {
    assertThat(SmokeCommand.principalFor(false)).isEqualTo("t1:smoke");
    String a = SmokeCommand.principalFor(true);
    assertThat(a).startsWith("t1:smoke-").isNotEqualTo(SmokeCommand.principalFor(true));
  }
}
