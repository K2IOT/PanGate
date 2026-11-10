package com.pangate.pushapi.http;

import com.pangate.pushapi.core.Caller;
import com.pangate.pushapi.store.RedisSeqEpochs;
import com.pangate.pushapi.store.SeqEpochWatcher;
import java.util.TreeMap;
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
  private final RedisSeqEpochs epochs;

  public AdminController(SeqEpochWatcher watcher, RedisSeqEpochs epochs) {
    this.watcher = watcher;
    this.epochs = epochs;
  }

  @PostMapping(path = "/admin/v1/seqepochs:bump", consumes = "application/json")
  public ResponseEntity<?> bump(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    Caller.ApiCaller c = (Caller.ApiCaller) req.getAttribute(ApiAuthFilter.CALLER);
    if (!"pangate".equals(c.tenant()) || !c.scopes().contains("admin")) {
      return ResponseEntity.status(403).body(Problems.of(403, "forbidden", "scope admin of tenant pangate required"));
    }
    int bumped;
    Set<Integer> slots = new HashSet<>();
    if (Boolean.TRUE.equals(body.get("all"))) {
      bumped = watcher.bumpAll();
      audit.info("seqepochs bump by {}: all {} slots", c.clientId(), bumped);
      // spec §3.4: shortened for all, the highest epoch instead of 16 384 entries
      return ResponseEntity.ok(Map.of("bumped", bumped, "maxEpoch", epochs.maxEpoch(0, 16_383)));
    } else if (body.get("slots") instanceof List<?> list && !list.isEmpty()) {
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
    Map<String, Integer> now = new TreeMap<>();
    slots.forEach(s -> now.put(Integer.toString(s), epochs.epochAt(s)));
    return ResponseEntity.ok(Map.of("bumped", bumped, "epochs", now));
  }
}
