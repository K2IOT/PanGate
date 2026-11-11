package com.pangate.pushapi.core;

import java.util.Set;

/** Who sends a push: a tenant backend over HTTP (API token) or an internal service over gRPC (SPIFFE id). */
public sealed interface Caller permits Caller.ApiCaller, Caller.InternalCaller {

  record ApiCaller(String tenant, String clientId, Set<String> scopes) implements Caller {}

  record InternalCaller(String spiffeId) implements Caller {}
}
