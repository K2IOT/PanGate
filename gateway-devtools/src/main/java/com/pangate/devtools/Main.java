package com.pangate.devtools;

import com.pangate.devtools.cmd.ClientCommand;
import com.pangate.devtools.cmd.HealthCommand;
import com.pangate.devtools.cmd.IssuerCommand;
import com.pangate.devtools.cmd.SmokeCommand;
import com.pangate.devtools.cmd.TokenCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "gateway-devtools", mixinStandardHelpOptions = true,
    description = "PanGate dev tools: issuer, token, client, smoke, health.",
    subcommands = {IssuerCommand.class, TokenCommand.class, ClientCommand.class, SmokeCommand.class,
        HealthCommand.class})
public final class Main {

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }
}
