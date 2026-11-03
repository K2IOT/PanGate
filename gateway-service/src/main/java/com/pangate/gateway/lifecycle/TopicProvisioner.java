package com.pangate.gateway.lifecycle;

import com.pangate.gateway.diagnostics.MissingTopicsException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/** Creates missing topics (local, dev) or refuses to start listing them (prod, spec §3.7). */
public final class TopicProvisioner {

  private TopicProvisioner() {}

  public static void ensure(Admin admin, TopicNames names, boolean create, Duration timeout) {
    try {
      Set<String> existing = admin.listTopics().names().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      List<NewTopic> missing = names.all().stream().filter(t -> !existing.contains(t.name())).toList();
      if (missing.isEmpty()) {
        return;
      }
      if (!create) {
        throw new MissingTopicsException(missing.stream().map(NewTopic::name).toList());
      }
      try {
        admin.createTopics(missing).all().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw e;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while provisioning Kafka topics", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException("cannot provision Kafka topics", e);
    }
  }
}
