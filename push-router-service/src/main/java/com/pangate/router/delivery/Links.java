package com.pangate.router.delivery;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.PushItem;
import java.util.concurrent.CompletableFuture;

/** Sends one item to one gateway node (implemented by GatewayLinks). */
public interface Links {

  /**
   * The item gets a fresh delivery_id. For RELIABLE / ORDERED MESSAGE items the future completes with the gateway's
   * AckItem status; for the others with STATUS_WRITTEN once handed to the stream. A lost stream completes pending
   * futures with STATUS_NACK_CLOSED.
   *
   * @throws com.pangate.router.links.NodeUnavailableException if the node is not live
   */
  CompletableFuture<AckItem.Status> send(String nodeId, PushItem item);
}
