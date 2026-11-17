package com.pangate.account.config;

import com.pangate.account.api.PortRoutingFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/** Servlet filters (account spec §1). */
@Configuration(proxyBeanMethods = false)
public class WebConfig {

  @Bean
  FilterRegistrationBean<PortRoutingFilter> portRoutingFilter(AdminConnector admin) {
    var r = new FilterRegistrationBean<>(new PortRoutingFilter(admin));
    r.setOrder(Ordered.HIGHEST_PRECEDENCE);
    r.addUrlPatterns("/*");
    return r;
  }
}
