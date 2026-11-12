package com.pangate.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.auth.application.ManageClients;
import com.pangate.auth.config.SeedRunner;
import com.pangate.auth.support.AuthIT;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;

@AuthIT
@TestPropertySource(properties = {"auth.seed.enabled=true", "auth.seed.dir=target/seed-it"})
class SeedIT {

  @Autowired SeedRunner seed;
  @Autowired ManageClients clients;
  @Autowired JdbcClient jdbc;
  @Autowired ObjectMapper json;

  @SuppressWarnings("unchecked")
  private Map<String, String> file() throws Exception {
    return json.readValue(Files.readString(Path.of("target/seed-it/clients.json")), Map.class);
  }

  @Test
  void seedIsIdempotentAndWritesClientsFileOnce() throws Exception {
    Map<String, String> first = file();  // written at startup
    assertThat(first).containsOnlyKeys("pangate-admin", "t1-backend", "chat-backend");
    assertThat(clients.authenticates("t1-backend", first.get("t1-backend").toCharArray())).isTrue();
    assertThat(clients.authenticates("pangate-admin", first.get("pangate-admin").toCharArray())).isTrue();
    String scopes = jdbc.sql("SELECT array_to_string(scopes, ' ') FROM auth.clients WHERE id = 't1-backend'")
        .query(String.class).single();
    assertThat(scopes).isEqualTo("tokens:mint sessions:revoke push:send push:broadcast");
    // app chat (chat spec §1, §8): tenant chat, client chat-backend mints connect tokens for its users
    assertThat(clients.authenticates("chat-backend", first.get("chat-backend").toCharArray())).isTrue();
    assertThat(jdbc.sql("SELECT array_to_string(scopes, ' ') FROM auth.clients WHERE id = 'chat-backend'")
        .query(String.class).single()).isEqualTo("tokens:mint sessions:revoke push:send push:broadcast");

    seed.seed();
    seed.seed();

    assertThat(file()).isEqualTo(first);
    assertThat(jdbc.sql("SELECT count(*) FROM auth.clients WHERE id IN ('t1-backend','pangate-admin')")
        .query(Long.class).single()).isEqualTo(2);
    assertThat(PosixPerms.of(Path.of("target/seed-it/clients.json"))).isEqualTo("rw-------");
  }

  static final class PosixPerms {
    static String of(Path p) throws Exception {
      return java.nio.file.attribute.PosixFilePermissions.toString(Files.getPosixFilePermissions(p));
    }
  }
}
