package com.pangate.account.api;

import com.pangate.account.application.Profiles;
import com.pangate.account.domain.DomainException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** {@code /v1/users*} (account spec §4.2): public profiles. */
@RestController
public class UsersController {

  private final Profiles profiles;

  public UsersController(Profiles profiles) {
    this.profiles = profiles;
  }

  static Map<String, Object> json(Profiles.PublicProfile p) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("userId", p.userId());
    if (p.deleted()) {
      m.put("deleted", true);
    } else {
      m.put("username", p.username());
      m.put("displayName", p.displayName());
      m.put("bio", p.bio());
    }
    return m;
  }

  @GetMapping("/v1/users/{userId}")
  public Map<String, Object> byId(@PathVariable("userId") String userId, HttpServletRequest req) {
    return json(profiles.byId(ConnectAuthFilter.caller(req), userId));
  }

  @GetMapping("/v1/users")
  public Map<String, Object> byUsername(@RequestParam(name = "username", required = false) String username,
      HttpServletRequest req) {
    return json(profiles.byUsername(ConnectAuthFilter.caller(req), username)
        .orElseThrow(() -> new DomainException("not_found", "no such user")));
  }

  @PostMapping(path = "/v1/users:batch", consumes = "application/json")
  public Map<String, Object> batch(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    if (!(body.get("userIds") instanceof List<?> ids) || !ids.stream().allMatch(String.class::isInstance)) {
      throw new DomainException("invalid_request", "userIds must be an array of strings");
    }
    List<String> userIds = ids.stream().map(String.class::cast).toList();
    return Map.of("users", profiles.batch(ConnectAuthFilter.caller(req), userIds).stream().map(UsersController::json)
        .toList());
  }
}
