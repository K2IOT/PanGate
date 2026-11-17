package com.pangate.devtools.cmd;

import java.net.URI;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** account-service by hand (account spec §4): register, log in, get a connect token. Prints JSON lines. */
@Command(name = "account", description = "account-service: register, login, token (password smoke-pw-1).",
    subcommands = {AccountCommand.Register.class, AccountCommand.Login.class, AccountCommand.Token.class})
public final class AccountCommand {

  @Option(names = "--url", required = true, scope = picocli.CommandLine.ScopeType.INHERIT,
      description = "account-service base URL, e.g. http://localhost:8088")
  URI url;

  AccountApi api() {
    return new AccountApi(url, null, null);
  }

  static String json(AccountApi.Account a) {
    return "{\"username\":\"" + a.username() + "\",\"userId\":\"" + a.userId() + "\",\"deviceId\":\""
        + a.deviceId() + "\",\"refreshToken\":\"" + a.refreshToken() + "\"}";
  }

  @Command(name = "register", description = "Create an account (email <username>@smoke.invalid).")
  static final class Register implements Callable<Integer> {
    @picocli.CommandLine.ParentCommand AccountCommand parent;
    @Option(names = "--username", required = true) String username;

    @Override
    public Integer call() {
      System.out.println(json(parent.api().register(username)));
      return 0;
    }
  }

  @Command(name = "login", description = "Log in as a new device.")
  static final class Login implements Callable<Integer> {
    @picocli.CommandLine.ParentCommand AccountCommand parent;
    @Option(names = "--username", required = true) String username;

    @Override
    public Integer call() {
      System.out.println(json(parent.api().login(username)));
      return 0;
    }
  }

  @Command(name = "token", description = "Exchange a refresh token for a 10-minute connect token.")
  static final class Token implements Callable<Integer> {
    @picocli.CommandLine.ParentCommand AccountCommand parent;
    @Option(names = "--refresh-token", required = true) String refreshToken;

    @Override
    public Integer call() {
      System.out.println(parent.api().connectToken(refreshToken));
      return 0;
    }
  }
}
