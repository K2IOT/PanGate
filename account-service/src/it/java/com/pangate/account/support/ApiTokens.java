package com.pangate.account.support;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Map;

/** API tokens of the seeded auth clients (chat-admin, chat-backend) from {@link AuthStack}. */
public final class ApiTokens {

  private ApiTokens() {}

  @SuppressWarnings("unchecked")
  public static String of(String clientId, String scope) {
    try {
      Map<String, String> clients = Http.JSON.readValue(Files.readString(AuthStack.clientsFile()), Map.class);
      String basic = Base64.getEncoder().encodeToString((clientId + ":" + clients.get(clientId))
          .getBytes(StandardCharsets.UTF_8));
      HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
              URI.create(AuthStack.baseUrl() + "/oauth2/token")).header("Authorization", "Basic " + basic)
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope="
              + URLEncoder.encode(scope, StandardCharsets.UTF_8))).build(), HttpResponse.BodyHandlers.ofString());
      JsonNode n = Http.JSON.readTree(r.body());
      return n.get("access_token").asText();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
