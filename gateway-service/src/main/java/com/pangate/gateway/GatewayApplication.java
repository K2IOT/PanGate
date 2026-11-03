package com.pangate.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Spring Boot owns lifecycle, configuration and the management port; Netty owns the data plane. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class GatewayApplication {

  public static void main(String[] args) {
    SpringApplication.run(GatewayApplication.class, args);
  }
}
