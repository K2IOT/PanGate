package com.pangate.chat.store;

import com.datastax.oss.driver.api.core.CqlSession;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Applies {@code cql/V<n>__*.cql} in order and records each in {@code schema_version} (chat spec §5). Each statement
 * is idempotent ({@code IF NOT EXISTS}), so a run that died half-way is repeated safely.
 */
public final class SchemaMigrator {

  static final List<String> MIGRATIONS = List.of("cql/V1__schema.cql", "cql/V2__users.cql", "cql/V3__state.cql",
      "cql/V4__presence_links.cql", "cql/V5__notifications.cql",
      "cql/V6__shown_delivered.cql",
      "cql/V7__who_can_add.cql",
      "cql/V8__owner.cql");

  private SchemaMigrator() {}

  /** Number of migrations applied by this call. */
  public static int migrate(CqlSession s, String keyspace) {
    s.execute(ScyllaSession.ddl("CREATE TABLE IF NOT EXISTS " + keyspace
        + ".schema_version (version int PRIMARY KEY, applied_at timestamp)"));
    int applied = 0;
    for (int i = 0; i < MIGRATIONS.size(); i++) {
      int version = i + 1;
      if (s.execute("SELECT version FROM " + keyspace + ".schema_version WHERE version = " + version).one() != null) {
        continue;
      }
      for (String stmt : statements(MIGRATIONS.get(i))) {
        try {
          s.execute(ScyllaSession.ddl(stmt.replace("${keyspace}", keyspace)));
        } catch (com.datastax.oss.driver.api.core.servererrors.InvalidQueryException e) {
          // a migration interrupted before its schema_version row: the column is there already
          if (!(stmt.strip().toUpperCase().startsWith("ALTER TABLE") && e.getMessage().toLowerCase().contains("exist"))) {
            throw e;
          }
        }
      }
      s.execute(com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(
          "INSERT INTO " + keyspace + ".schema_version (version, applied_at) VALUES (?, ?)", version, Instant.now()));
      applied++;
    }
    return applied;
  }

  static List<String> statements(String resource) {
    try (InputStream in = SchemaMigrator.class.getClassLoader().getResourceAsStream(resource)) {
      String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      List<String> out = new ArrayList<>();
      StringBuilder cur = new StringBuilder();
      for (String line : text.split("\n")) {
        if (line.strip().startsWith("--")) {
          continue;
        }
        cur.append(line).append('\n');
        if (line.strip().endsWith(";")) {
          out.add(cur.toString().strip().replaceAll(";$", ""));
          cur.setLength(0);
        }
      }
      return out;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
