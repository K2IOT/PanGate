package com.pangate.chat.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.pangate.chat.config.ChatProperties;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.scylladb.ScyllaDBContainer;

class SchemaMigratorIT {

  static final ScyllaDBContainer scylla = new ScyllaDBContainer("scylladb/scylla:6.2.3");
  static CqlSession session;

  @BeforeAll
  static void boot() {
    scylla.start();
    ChatProperties.Scylla cfg = new ChatProperties.Scylla(
        List.of(scylla.getHost() + ":" + scylla.getMappedPort(9042)), "datacenter1", "chat_it", Duration.ofSeconds(5), 1);
    session = ScyllaSession.connect(cfg);
  }

  @AfterAll
  static void shutdown() {
    session.close();
    scylla.stop();
  }

  @Test
  void migrationIsIdempotent() {
    assertThat(SchemaMigrator.migrate(session, "chat_it")).isEqualTo(1);
    assertThat(SchemaMigrator.migrate(session, "chat_it")).isZero();
    Set<String> tables = session.execute(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'chat_it'").all().stream()
        .map(r -> r.getString("table_name")).collect(Collectors.toSet());
    assertThat(tables).contains("messages", "conversations", "members", "user_convs", "dm_index", "send_dedup",
        "schema_version");
    int ttl = session.execute("SELECT default_time_to_live FROM system_schema.tables "
        + "WHERE keyspace_name = 'chat_it' AND table_name = 'send_dedup'").one().getInt(0);
    assertThat(ttl).isEqualTo(86_400);
  }
}
