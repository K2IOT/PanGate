package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.PushBatch;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.ConnId;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import io.micrometer.core.instrument.Counter;

/**
 * Splits a {@link PushBatch} by event loop on the gRPC executor (spec §6.3): MESSAGE by the loop in its connId,
 * BROADCAST to every loop (encoded once), CONTROL to the loops holding the principal. Each loop gets one task.
 */
public final class PushDispatcher {

  static final int SHED_NACK_LEVEL = 4;

  private final LoopRegistry loops;
  private final PrincipalIndex principals;
  private final NodeState node;
  private final LoopItemHandler handler;
  private final Counter droppedShed;
  private final Counter droppedNotFound;
  private final Counter badItem;
  private final Counter itemErrors;
  private final java.util.Map<com.pangate.contracts.v1.ControlOp.OpCase, Counter> controlOps =
      new java.util.EnumMap<>(com.pangate.contracts.v1.ControlOp.OpCase.class);

  public PushDispatcher(LoopRegistry loops, PrincipalIndex principals, NodeState node, PushDelivery delivery,
      ControlPushHandler control, GatewayMetrics m) {
    this(loops, principals, node, LoopItemHandler.of(delivery, control), m);
  }

  public PushDispatcher(LoopRegistry loops, PrincipalIndex principals, NodeState node, LoopItemHandler handler,
      GatewayMetrics m) {
    this.loops = loops;
    this.principals = principals;
    this.node = node;
    this.handler = handler;
    this.droppedShed = m.counter("gw.push.dropped", "reason", "shed");
    this.droppedNotFound = m.counter("gw.push.dropped", "reason", "not_found");
    this.badItem = m.counter("gw.push.dropped", "reason", "bad_item");
    this.itemErrors = m.counter("gw.push.dropped", "reason", "error");
    for (com.pangate.contracts.v1.ControlOp.OpCase op : com.pangate.contracts.v1.ControlOp.OpCase.values()) {
      if (op != com.pangate.contracts.v1.ControlOp.OpCase.OP_NOT_SET) {
        controlOps.put(op, m.counter("gw.control.push", "op", op.name().toLowerCase(java.util.Locale.ROOT)));
      }
    }
  }

  public void dispatch(PushBatch batch, StreamRef stream) {
    LoopBatchTask[] tasks = new LoopBatchTask[loops.size()];
    for (PushItem item : batch.getItemsList()) {
      switch (item.getKind()) {
        case KIND_MESSAGE -> message(item, stream, tasks);
        case KIND_BROADCAST -> {
          if (item.getTopic().isEmpty()) {
            badItem.increment();
            continue;
          }
          PreEncodedFrame frame = PreEncodedFrame.encode(item.getFrame(), item.getTenantId(), loops.size());
          for (int i = 0; i < tasks.length; i++) {
            task(tasks, i, stream).add(item, frame);
          }
        }
        case KIND_CONTROL -> {
          Counter op = controlOps.get(item.getControl().getOpCase());
          if (op != null) {
            op.increment();
          }
          boolean[] seen = new boolean[tasks.length];
          for (ConnRef ref : principals.get(item.getPrincipalId())) {
            if (ref.loopIdx() < tasks.length && !seen[ref.loopIdx()]) {
              seen[ref.loopIdx()] = true;
              task(tasks, ref.loopIdx(), stream).add(item, null);
            }
          }
        }
        default -> {
          badItem.increment();
          if (PushDelivery.reliable(item.getFrame().getClass_())) {
            stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_NACK_NOT_FOUND);
          }
        }
      }
    }
    for (int i = 0; i < tasks.length; i++) {
      if (tasks[i] != null) {
        try {
          loops.get(i).eventLoop().execute(tasks[i]);
        } catch (java.util.concurrent.RejectedExecutionException e) {
          tasks[i].rejected();  // data plane already stopped (shutdown)
        }
      }
    }
  }

  private void message(PushItem item, StreamRef stream, LoopBatchTask[] tasks) {
    boolean reliable = PushDelivery.reliable(item.getFrame().getClass_());
    if (node.shedLevel() >= SHED_NACK_LEVEL) {
      if (reliable) {
        stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_NACK_SHED);
      } else {
        droppedShed.increment();
      }
      return;
    }
    String connId = item.getConnId();
    int idx = ConnId.isValid(connId) ? ConnId.loopIdx(connId) : -1;
    if (idx < 0 || idx >= tasks.length) {
      if (reliable) {
        stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_NACK_NOT_FOUND);
      } else {
        droppedNotFound.increment();
      }
      return;
    }
    task(tasks, idx, stream).add(item, null);
  }

  private LoopBatchTask task(LoopBatchTask[] tasks, int idx, StreamRef stream) {
    if (tasks[idx] == null) {
      tasks[idx] = new LoopBatchTask(loops.get(idx), handler, stream, itemErrors);
    }
    return tasks[idx];
  }
}
