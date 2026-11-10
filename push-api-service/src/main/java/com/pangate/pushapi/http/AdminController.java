package com.pangate.pushapi.http;

import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.store.SeqEpochWatcher;
import jakarta.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /admin/v1/seqepochs:bump} on the management port (push-api spec §3.4). */
@RestController
public class AdminController {

  private static final Logger audit = LoggerFactory.getLogger("pushapi.audit");

  private final SeqEpochWatcher watcher;

  public AdminController(SeqEpochWatcher watcher) {
    this.watcher = watcher;
  }

  @PostMapping(path = "/admin/v1/seqepochs:bump", consumes = "application/json")
  public ResponseEntity<?> bump(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    Caller.ApiCaller c = (Caller.ApiCaller) req.getAttribute(ApiAuthFilter.CALLER);
    if (!"pangate".equals(c.tenant()) || !c.scopes().contains("admin")) {
      return ResponseEntity.status(403).body(Problems.of(403, "forbidden", "scope admin of tenant pangate required"));
    }
    int bumped;
    if (Boolean.TRUE.equals(body.get("all"))) {
      bumped = watcher.bumpAll();
    } else if (body.get("slots") instanceof List<?> list && !list.isEmpty()) {
      Set<Integer> slots = new HashSet<>();
      for (Object o : list) {
        if (!(o instanceof Integer i) || i < 0 || i > 16_383) {
          return ResponseEntity.badRequest().body(Problems.of(400, "invalid_request", "slots are integers 0..16383"));
        }
        slots.add(i);
      }
      bumped = watcher.bump(slots);
    } else {
      return ResponseEntity.badRequest().body(Problems.of(400, "invalid_request", "give slots or all=true"));
    }
    audit.info("seqepochs bump by {}: {} slots", c.clientId(), bumped);
    return ResponseEntity.ok(Map.of("bumped", bumped));
  }
}
