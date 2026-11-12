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

  private ScyllaSession() {}

  public static CqlSession connect(ChatProperties.Scylla cfg) {
    CqlSessionBuilder b = CqlSession.builder().withLocalDatacenter(cfg.localDc())
        .withConfigLoader(DriverConfigLoader.programmaticBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, cfg.timeout())
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, cfg.timeout().multipliedBy(5))
            .build());
    for (String cp : cfg.contactPoints()) {
      int colon = cp.lastIndexOf(':');
      b.addContactPoint(InetSocketAddress.createUnresolved(cp.substring(0, colon),
          Integer.parseInt(cp.substring(colon + 1))));
    }
    CqlSession s = b.build();
    s.execute("CREATE KEYSPACE IF NOT EXISTS " + cfg.keyspace() + " WITH replication = {'class': "
        + "'NetworkTopologyStrategy', '" + cfg.localDc() + "': " + cfg.replicationFactor() + "} "
        // LWT (conversations.last_seq fence, dm_index) is not supported on tablets in ScyllaDB 6.x
        + "AND tablets = {'enabled': false}");
    return s;
  }
}
