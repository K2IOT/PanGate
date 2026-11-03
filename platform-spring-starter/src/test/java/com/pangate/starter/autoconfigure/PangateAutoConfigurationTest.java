package com.pangate.starter.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.starter.logging.PrincipalHasher;
import com.pangate.starter.secrets.SecretSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class PangateAutoConfigurationTest {

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(
          PangateSecretsAutoConfiguration.class, PangateObservabilityAutoConfiguration.class));

  @Test
  @SuppressWarnings("unchecked")
  void commonTagsApplied() {
    runner.withPropertyValues("pangate.cell=cell-1", "pangate.az=az-a", "pangate.node=n1", "pangate.service=gw")
        .run(ctx -> {
          MeterRegistry registry = new SimpleMeterRegistry();
          ctx.getBean(MeterRegistryCustomizer.class).customize(registry);
          registry.counter("x").increment();

          assertThat(registry.get("x").counter().getId().getTag("cell")).isEqualTo("cell-1");
          assertThat(registry.get("x").counter().getId().getTag("az")).isEqualTo("az-a");
          assertThat(registry.get("x").counter().getId().getTag("node")).isEqualTo("n1");
          assertThat(registry.get("x").counter().getId().getTag("service")).isEqualTo("gw");
        });
  }

  @Test
  @SuppressWarnings("unchecked")
  void emptyTagsAreSkipped() {
    runner.withPropertyValues("pangate.cell=cell-1").run(ctx -> {
      MeterRegistry registry = new SimpleMeterRegistry();
      ctx.getBean(MeterRegistryCustomizer.class).customize(registry);
      registry.counter("x").increment();

      assertThat(registry.get("x").counter().getId().getTag("cell")).isEqualTo("cell-1");
      assertThat(registry.get("x").counter().getId().getTag("az")).isNull();
    });
  }

  @Test
  void secretSourceUsesConfiguredDir(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("s"), "v");
    runner.withPropertyValues("pangate.secrets.dir=" + dir).run(ctx ->
        assertThat(new String(ctx.getBean(SecretSource.class).get("s"))).isEqualTo("v"));
  }

  @Test
  void principalHasherIsLazyAndReadsKey(@TempDir Path dir) throws Exception {
    runner.withPropertyValues("pangate.secrets.dir=" + dir).run(ctx -> {
      assertThat(ctx).hasNotFailed();
      Files.writeString(dir.resolve("principal-hash.key"), Base64.getEncoder().encodeToString(new byte[32]));
      ObjectProvider<PrincipalHasher> provider = ctx.getBeanProvider(PrincipalHasher.class);
      assertThat(provider.getObject().hash("t1:u1")).hasSize(16);
    });
  }
}
