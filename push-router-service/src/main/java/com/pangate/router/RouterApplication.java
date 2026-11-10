package com.pangate.router;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RouterApplication {

  public static void main(String[] args) {
    SpringApplication.run(RouterApplication.class, args);
  }
}
