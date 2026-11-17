package com.pangate.notify;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Push tokens per device and notifications to APNs / FCM for every tenant (offline notifications spec §5). */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class NotifyApplication {

  public static void main(String[] args) {
    SpringApplication.run(NotifyApplication.class, args);
  }
}
