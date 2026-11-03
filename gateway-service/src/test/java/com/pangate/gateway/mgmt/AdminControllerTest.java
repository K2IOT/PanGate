package com.pangate.gateway.mgmt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AdminControllerTest {

  private final AtomicInteger drains = new AtomicInteger();
  private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new AdminController(drains::incrementAndGet)).build();

  @Test
  void drainRejectsNonLoopback() throws Exception {
    mvc.perform(post("/admin/drain").with(r -> {
      r.setRemoteAddr("10.0.0.5");
      return r;
    })).andExpect(status().isForbidden());

    assertThat(drains.get()).isZero();
  }

  @Test
  void drainAcceptsLoopback() throws Exception {
    mvc.perform(post("/admin/drain").with(r -> {
      r.setRemoteAddr("127.0.0.1");
      return r;
    })).andExpect(status().isAccepted());

    org.awaitility.Awaitility.await().until(() -> drains.get() == 1);
  }
}
