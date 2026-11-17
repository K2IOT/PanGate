package com.pangate.account.config;

import com.pangate.account.application.port.AccountStore;
import com.pangate.account.application.port.AdminLog;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.SessionStore;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.infrastructure.postgres.JdbcAccountStore;
import com.pangate.account.infrastructure.postgres.JdbcAdminLog;
import com.pangate.account.infrastructure.postgres.JdbcOutbox;
import com.pangate.account.infrastructure.postgres.JdbcSessionStore;
import java.util.function.Supplier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** PostgreSQL adapters (account spec §3). */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfig {

  @Bean
  AccountStore accountStore(JdbcClient jdbc) {
    return new JdbcAccountStore(jdbc);
  }

  @Bean
  SessionStore sessionStore(JdbcClient jdbc) {
    return new JdbcSessionStore(jdbc);
  }

  @Bean
  Outbox outbox(JdbcClient jdbc) {
    return new JdbcOutbox(jdbc);
  }

  @Bean
  AdminLog adminLog(JdbcClient jdbc) {
    return new JdbcAdminLog(jdbc);
  }

  @Bean
  Transactions transactions(PlatformTransactionManager tm) {
    TransactionTemplate t = new TransactionTemplate(tm);
    return new Transactions() {
      @Override
      public <T> T inTx(Supplier<T> work) {
        return t.execute(s -> work.get());
      }

      @Override
      public void inTx(Runnable work) {
        t.executeWithoutResult(s -> work.run());
      }
    };
  }
}
