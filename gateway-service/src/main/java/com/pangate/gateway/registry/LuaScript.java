package com.pangate.gateway.registry;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A Lua script run with EVALSHA; on NOSCRIPT (fresh or flushed server) it falls back to EVAL, which caches it. */
final class LuaScript {

  private final String text;
  private final String sha;
  private final ScriptOutputType type;

  LuaScript(String text, ScriptOutputType type) {
    this.text = text;
    this.type = type;
    try {
      this.sha = HexFormat.of().formatHex(
          MessageDigest.getInstance("SHA-1").digest(text.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  <T> CompletionStage<T> run(RedisClusterAsyncCommands<String, String> cmds, String key, String... args) {
    String[] keys = {key};
    CompletionStage<T> first;
    try {
      first = cmds.<T>evalsha(sha, type, keys, args);
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
    return first.handle((v, e) -> {
      if (e == null) {
        return CompletableFuture.completedFuture(v);
      }
      Throwable cause = e instanceof java.util.concurrent.CompletionException ? e.getCause() : e;
      if (cause instanceof RedisNoScriptException) {
        return cmds.<T>eval(text, type, keys, args);
      }
      return CompletableFuture.<T>failedFuture(cause);
    }).thenCompose(s -> s);
  }
}
