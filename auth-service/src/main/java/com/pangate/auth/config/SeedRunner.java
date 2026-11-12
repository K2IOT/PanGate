package com.pangate.auth.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pangate.auth.application.ManageClients;
import com.pangate.auth.application.ManageTenants;
import com.pangate.auth.domain.Scope;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Local/dev seed (spec §8), idempotent: tenant {@code pangate} + client {@code pangate-admin} (admin) and tenant
 * {@code t1} + client {@code t1-backend}, tenant {@code chat} + client {@code chat-backend} (app chat). Secrets live in {@code <seed.dir>/clients.json} for devtools; an existing
 * file wins, so a recreated database accepts the secrets the tools already have.
 */
@Component
@ConditionalOnProperty(name = "auth.seed.enabled", havingValue = "true")
public class SeedRunner implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(SeedRunner.class);

  private record SeedClient(String tenant, String name, Set<Scope> scopes) {
    String id() {
      return tenant + "-" + name;
    }
  }

  private static final SeedClient[] CLIENTS = {
      new SeedClient("pangate", "admin", EnumSet.of(Scope.ADMIN)),
      new SeedClient("t1", "backend", EnumSet.of(Scope.TOKENS_MINT, Scope.SESSIONS_REVOKE, Scope.PUSH_SEND,
          Scope.PUSH_BROADCAST)),
      // app chat (chat spec §1, §8): one tenant for the whole app; its backend mints connect tokens for users
      new SeedClient("chat", "backend", EnumSet.of(Scope.TOKENS_MINT, Scope.SESSIONS_REVOKE, Scope.PUSH_SEND,
          Scope.PUSH_BROADCAST))};

  private final ManageTenants tenants;
  private final ManageClients clients;
  private final ObjectMapper json;
  private final Path file;

  public SeedRunner(ManageTenants tenants, ManageClients clients, ObjectMapper json, AuthProperties props) {
    this.tenants = tenants;
    this.clients = clients;
    this.json = json;
    this.file = props.seed().dir().resolve("clients.json");
  }

  @Override
  public void run(ApplicationArguments args) {
    seed();
  }

  public synchronized void seed() {
    Map<String, String> secrets = read();
    boolean fileChanged = false;
    for (SeedClient c : CLIENTS) {
      tenants.ensure(c.tenant());
      String secret = secrets.get(c.id());
      if (secret == null) {
        secret = ManageClients.newSecret();
        secrets.put(c.id(), secret);
        fileChanged = true;
      }
      if (clients.ensure(c.tenant(), c.name(), c.scopes(), secret)) {
        log.info("seed client {} written", c.id());
      }
    }
    if (fileChanged) {
      write(secrets);
      log.info("seed client secrets written to {}", file.toAbsolutePath());
    }
  }

  private Map<String, String> read() {
    if (!Files.exists(file)) {
      return new LinkedHashMap<>();
    }
    try {
      return new LinkedHashMap<>(json.readValue(file.toFile(), new TypeReference<Map<String, String>>() {}));
    } catch (IOException e) {
      throw new UncheckedIOException("cannot read seed file " + file, e);
    }
  }

  private void write(Map<String, String> secrets) {
    try {
      Files.createDirectories(file.toAbsolutePath().getParent());
      Path tmp = Files.createTempFile(file.toAbsolutePath().getParent(), "clients", ".tmp",
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      json.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), secrets);
      Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException("cannot write seed file " + file, e);
    }
  }
}
