package com.pangate.devtools.cmd;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "health", description = "Exit 0 if the URL answers HTTP 200 within 2 s, else 1.")
public final class HealthCommand implements Callable<Integer> {

  @Parameters(index = "0")
  URI url;

  @Override
  public Integer call() {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      HttpResponse<Void> res = client.send(
          HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.discarding());
      return res.statusCode() == 200 ? 0 : 1;
    } catch (Exception e) {
      return 1;
    }
  }
}
