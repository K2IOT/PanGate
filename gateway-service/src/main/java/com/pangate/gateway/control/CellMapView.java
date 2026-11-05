package com.pangate.gateway.control;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tenant → cell placement from gw.control (spec §6.8). A tenant with no mapping is served by this cell, so local and
 * dev work before any CellMap event exists.
 */
public final class CellMapView {

  private final String ownCell;
  private final ConcurrentHashMap<String, String> cells = new ConcurrentHashMap<>();

  public CellMapView(String ownCell) {
    this.ownCell = ownCell;
  }

  public void put(String tenant, String cell) {
    cells.put(tenant, cell);
  }

  public void remove(String tenant) {
    cells.remove(tenant);
  }

  public boolean servesTenant(String tenant) {
    String c = cells.get(tenant);
    return c == null || c.equals(ownCell);
  }

  public Optional<String> cellOf(String tenant) {
    return Optional.ofNullable(cells.get(tenant));
  }

  public String ownCell() {
    return ownCell;
  }
}
