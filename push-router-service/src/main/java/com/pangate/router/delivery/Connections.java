package com.pangate.router.delivery;

import com.pangate.router.registry.LiveConnection;
import java.util.List;

/** Live connections of a principal (implemented by RegistryLookup). */
public interface Connections {

  /** @throws com.pangate.router.registry.RegistryUnavailableException on a registry timeout or error */
  List<LiveConnection> live(String principal);

  void invalidate(String principal);

  /** Every session of {@code principal} on a live node at its current epoch, draining nodes included (CONTROL). */
  List<LiveConnection> sessions(String principal);
}
