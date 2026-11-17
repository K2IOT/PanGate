package com.pangate.notify.config;

import com.pangate.notify.api.PortRoutingFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/** Servlet filters (offline notifications spec §5.1). */
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
