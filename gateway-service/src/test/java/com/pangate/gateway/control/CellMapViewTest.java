package com.pangate.gateway.control;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CellMapViewTest {

  @Test
  void unmappedTenantIsServedAndForeignCellIsNot() {
    CellMapView cells = new CellMapView("cell-1");
    cells.put("t1", "cell-1");
    cells.put("t2", "cell-2");

    assertThat(cells.servesTenant("t1")).isTrue();
    assertThat(cells.servesTenant("t2")).isFalse();
    assertThat(cells.servesTenant("t3")).isTrue();
    assertThat(cells.cellOf("t2")).hasValue("cell-2");
    assertThat(cells.cellOf("t3")).isEmpty();

    cells.remove("t2");
    assertThat(cells.servesTenant("t2")).isTrue();
  }
}
