package com.pangate.starter.autoconfigure;

import com.pangate.starter.logging.PrincipalHasher;
import com.pangate.starter.secrets.FileSecretSource;
import com.pangate.starter.secrets.SecretSource;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

/** Secret source and principal hasher. */
@AutoConfiguration
@EnableConfigurationProperties(PangateProperties.class)
public class PangateSecretsAutoConfiguration {

  static final String PRINCIPAL_HASH_KEY = "principal-hash.key";

  @Bean
  @ConditionalOnMissingBean
  @ConditionalOnProperty(name = "pangate.secrets.source", havingValue = "file", matchIfMissing = true)
  SecretSource fileSecretSource(PangateProperties props) {
    return new FileSecretSource(props.secrets().dir());
  }

  /** Lazy so that services start before the key exists; resolved on first use. */
  @Bean
  @Lazy
  @ConditionalOnMissingBean
  PrincipalHasher principalHasher(SecretSource secrets) {
    String encoded = new String(secrets.get(PRINCIPAL_HASH_KEY), StandardCharsets.US_ASCII).trim();
    return new PrincipalHasher(Base64.getDecoder().decode(encoded));
  }
}
