package com.pangate.chat.config;

import org.apache.catalina.connector.Connector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

/**
 * Second Tomcat connector for the management port (chat-service spec §1: admin and actuator on :8081). One server with two
 * connectors keeps the admin endpoints inside the application context (filters, controllers, beans), which a separate
 * management child context would not; {@code PortRoutingFilter} keeps each path on its own port.
 */
@Component
public class AdminConnector implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

  private final Connector connector;

  public AdminConnector(@Value("${chat.management-port:8081}") int port) {
    connector = new Connector(TomcatServletWebServerFactory.DEFAULT_PROTOCOL);
    connector.setPort(port);
  }

  @Override
  public void customize(TomcatServletWebServerFactory factory) {
    factory.addAdditionalTomcatConnectors(connector);
  }

  /** The bound port (resolves port 0 after start). */
  public int port() {
    return connector.getLocalPort();
  }
}
