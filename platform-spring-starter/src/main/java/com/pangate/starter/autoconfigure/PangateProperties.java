package com.pangate.starter.autoconfigure;

import java.nio.file.Path;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Platform-wide identity and secret settings shared by every PanGate service. */
@ConfigurationProperties("pangate")
public record PangateProperties(
    String cell, String az, String node, String service, @DefaultValue Secrets secrets) {

  public record Secrets(@DefaultValue("file") String source, @DefaultValue("/etc/pangate/secrets") Path dir) {}
}
