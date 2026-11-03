package com.pangate.gateway.session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The only node-wide session index (spec §3.4): principalId → its sessions on this node. The value is a single
 * {@link ConnRef} (common case) or a {@code ConnRef[]} (multi-device), updated atomically with {@code compute}.
 */
public final class PrincipalIndex {

  private final ConcurrentHashMap<String, Object> index = new ConcurrentHashMap<>();

  public void add(String principal, ConnRef ref) {
    index.compute(principal, (k, v) -> {
      if (v == null) {
        return ref;
      }
      if (v instanceof ConnRef single) {
        return new ConnRef[] {single, ref};
      }
      ConnRef[] arr = (ConnRef[]) v;
      ConnRef[] grown = java.util.Arrays.copyOf(arr, arr.length + 1);
      grown[arr.length] = ref;
      return grown;
    });
  }

  public void remove(String principal, ConnRef ref) {
    index.computeIfPresent(principal, (k, v) -> {
      if (v instanceof ConnRef single) {
        return single.equals(ref) ? null : single;
      }
      List<ConnRef> left = new ArrayList<>();
      for (ConnRef r : (ConnRef[]) v) {
        if (!r.equals(ref)) {
          left.add(r);
        }
      }
      return switch (left.size()) {
        case 0 -> null;
        case 1 -> left.get(0);
        default -> left.toArray(new ConnRef[0]);
      };
    });
  }

  public List<ConnRef> get(String principal) {
    Object v = index.get(principal);
    if (v == null) {
      return List.of();
    }
    return v instanceof ConnRef single ? List.of(single) : List.of((ConnRef[]) v);
  }

  public int principalCount() {
    return index.size();
  }
}
