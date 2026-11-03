package com.pangate.gateway.mgmt;

import jakarta.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** Loopback-only admin actions on the management port (spec §7.9). */
@RestController
public class AdminController {

  private final Runnable drain;

  public AdminController(Runnable drain) {
    this.drain = drain;
  }

  /** Used by the Kubernetes preStop hook. M1 stops the data plane; M3 replaces this with the staged drain. */
  @PostMapping("/admin/drain")
  public ResponseEntity<Void> drain(HttpServletRequest request) {
    if (!isLoopback(request.getRemoteAddr())) {
      return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }
    Thread.ofVirtual().name("gw-drain").start(drain);
    return ResponseEntity.accepted().build();
  }

  private static boolean isLoopback(String addr) {
    try {
      return addr != null && InetAddress.getByName(addr).isLoopbackAddress();
    } catch (UnknownHostException e) {
      return false;
    }
  }
}
