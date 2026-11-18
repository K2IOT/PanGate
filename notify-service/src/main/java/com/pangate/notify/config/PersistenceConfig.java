package com.pangate.notify.config;

import com.pangate.notify.application.port.RateLimiter;
import com.pangate.notify.application.port.TokenStore;
import com.pangate.notify.infrastructure.postgres.JdbcTokenStore;
import com.pangate.notify.infrastructure.redis.RedisRateLimiter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import com.pangate.notify.infrastructure.redis.LazyRedis;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** PostgreSQL and Redis adapters (offline notifications spec §5.2, §5.4). */
@Configuration(proxyBeanMethods = false)
public class PersistenceConfig {

  @Bean
  TokenStore tokenStore(JdbcClient jdbc, PlatformTransactionManager tm) {
    return new JdbcTokenStore(jdbc, new TransactionTemplate(tm));
  }

  @Bean(destroyMethod = "shutdown")
  RedisClient redisClient(NotifyProperties p) {
    RedisURI uri = RedisURI.create(p.redis().uri());
    uri.setTimeout(p.redis().timeout());
    return RedisClient.create(uri);
  }

  @Bean(destroyMethod = "close")
  LazyRedis lazyRedis(RedisClient client) {
    return new LazyRedis(client);
  }

  @Bean
  RateLimiter rateLimiter(LazyRedis redis) {
    return new RedisRateLimiter(redis);
  }
}
