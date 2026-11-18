package com.pangate.notify.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

/** Full application on random ports against the shared Postgres, Kafka and Redis containers and {@link FakeAuth}. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"notify.admin-port=0"})
@ContextConfiguration(initializers = ContainersInitializer.class)
public @interface NotifyIT {}
