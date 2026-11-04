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
}
