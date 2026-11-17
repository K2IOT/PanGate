package com.pangate.account;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Accounts of the app chat (account spec): users, passwords, device sessions, connect tokens through auth-service. */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class AccountApplication {

  public static void main(String[] args) {
    SpringApplication.run(AccountApplication.class, args);
  }
}
