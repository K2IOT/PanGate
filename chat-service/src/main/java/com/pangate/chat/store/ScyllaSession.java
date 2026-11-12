package com.pangate.chat.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.pangate.chat.config.ChatProperties;
import java.net.InetSocketAddress;

/**
 * A session on the chat keyspace's cluster; the keyspace is created here if missing (chat spec §5), with vnodes
 * rather than tablets because the sequencer's fence is an LWT.
 */
public final class ScyllaSession {

  /** Schema changes wait for agreement across the cluster: far longer than the 1 s of an ordinary request. */
  public static final java.time.Duration DDL_TIMEOUT = java.time.Duration.ofSeconds(30);

  private ScyllaSession() {}

  /** A schema statement with {@link #DDL_TIMEOUT}. */
  public static com.datastax.oss.driver.api.core.cql.SimpleStatement ddl(String cql) {
    return com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(cql).setTimeout(DDL_TIMEOUT);
  }

  public static CqlSession connect(ChatProperties.Scylla cfg) {
    CqlSessionBuilder b = CqlSession.builder().withLocalDatacenter(cfg.localDc())
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, cfg.timeout())
            // with RF 3 a LOCAL_ONE read can miss a row a LOCAL_ONE write just made (a "missing" conversation
            // re-created, a removed member read back): quorum for both, local serial for the LWTs
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
            .withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, "LOCAL_SERIAL")
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, cfg.timeout().multipliedBy(5))
            .build());
    for (String cp : cfg.contactPoints()) {
      int colon = cp.lastIndexOf(':');
      b.addContactPoint(InetSocketAddress.createUnresolved(cp.substring(0, colon),
          Integer.parseInt(cp.substring(colon + 1))));
    }
    CqlSession s = b.build();
    s.execute(ddl("CREATE KEYSPACE IF NOT EXISTS " + cfg.keyspace() + " WITH replication = {'class': "
        + "'NetworkTopologyStrategy', '" + cfg.localDc() + "': " + cfg.replicationFactor() + "} "
        // LWT (conversations.last_seq fence, dm_index) is not supported on tablets in ScyllaDB 6.x
        + "AND tablets = {'enabled': false}"));
    return s;
  }
}
