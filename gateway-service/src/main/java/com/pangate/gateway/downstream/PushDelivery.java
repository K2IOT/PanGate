package com.pangate.gateway.downstream;

import com.pangate.contracts.v1.Ack;
import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.PushItem;
import com.pangate.gateway.config.LimitsHolder;
import com.pangate.gateway.control.TenantLimitsStore;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.metrics.GatewayMetrics;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.ConnId;
import com.pangate.gateway.session.PendingAckWindow;
import com.pangate.gateway.session.PendingEntry;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.session.SessionState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Unicast and RPC_REQ delivery on the owner loop (spec §5.3) and the client's ACK (spec §5.4). RELIABLE / ORDERED
 * get exactly one ack per delivery id (written, acked, or a NACK); EPHEMERAL never get one.
 */
public final class PushDelivery {

  static final int MAX_ACK_IDS = 32;

  private final TenantLimitsStore tenants;
  private final LimitsHolder limits;
  private final LongSupplier wallMillis;
  private final Counter droppedUnwritable;
  private final Counter droppedNotFound;
  private final Timer handoff;

  public PushDelivery(TenantLimitsStore tenants, LimitsHolder limits, GatewayMetrics m, LongSupplier wallMillis) {
    this.tenants = tenants;
    this.limits = limits;
    this.wallMillis = wallMillis;
    this.droppedUnwritable = m.counter("gw.push.dropped", "reason", "unwritable");
    this.droppedNotFound = m.counter("gw.push.dropped", "reason", "not_found");
    this.handoff = Timer.builder("gw.push.handoff").register(m.registry());
  }

  public static boolean reliable(DeliveryClass c) {
    return c == DeliveryClass.DELIVERY_CLASS_RELIABLE || c == DeliveryClass.DELIVERY_CLASS_ORDERED;
  }

  /** On the owner loop. Returns the channel written to (the caller flushes it at the end of its task), or null. */
  public Channel deliver(LoopContext loop, PushItem item, StreamRef stream) {
    Frame frame = item.getFrame();
    boolean reliable = reliable(frame.getClass_());
    String connId = item.getConnId();
    Session s = ConnId.isValid(connId) ? loop.session(ConnId.sessionKey(connId)) : null;
    if (s == null || !connId.equals(s.connId())
        || (s.state() != SessionState.ACTIVE && s.state() != SessionState.DRAINING)) {
      if (reliable) {
        stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_NACK_NOT_FOUND);
      } else {
        droppedNotFound.increment();
      }
      return null;
    }
    Channel ch = s.channel();
    // SSE clients cannot ACK (plan ruling R1): RELIABLE is acknowledged as WRITTEN, like downgradeReliable.
    if (!reliable || tenants.policy(s.identity().tenantId()).downgradeReliable()
        || ch.attr(com.pangate.gateway.protocol.ChannelAttrs.TRANSPORT).get()
            == com.pangate.gateway.protocol.ChannelAttrs.TransportKind.SSE) {
      boolean written = false;
      if (ch.isWritable()) {
        write(ch, item);
        written = true;
      } else {
        droppedUnwritable.increment();
      }
      if (reliable) {
        stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_WRITTEN);
      }
      return written ? ch : null;
    }
    PendingAckWindow w = s.pending();
    if (w != null && w.find(frame.getMsgId()) != null) {
      w.replace(frame.getMsgId(), item.getDeliveryId(), stream);
      return null;
    }
    int bytes = frame.getSerializedSize();
    if (w != null && !w.fits(bytes)) {
      stream.ack(item.getDeliveryId(), AckItem.Status.STATUS_NACK_OVERFLOW);
      if (s.countOverflowNack(loop.nowSec())) {
        s.close(CloseCode.SLOW_CONSUMER, "PENDING_OVERFLOW");
      }
      return null;
    }
    s.addPending(new PendingEntry(frame.getMsgId(), item.getDeliveryId(), stream, loop.nowSec(), bytes));
    write(ch, item);
    return ch;
  }

  private void write(Channel ch, PushItem item) {
    ch.write(item.getFrame(), ch.voidPromise());
    Session s = ch.attr(Session.KEY).get();
    if (s != null) {
      s.loop().markDirty(s);
    }
    if (item.getEnqueuedAtMs() > 0) {
      handoff.record(Math.max(0, wallMillis.getAsLong() - item.getEnqueuedAtMs()), TimeUnit.MILLISECONDS);
    }
  }

  /** ACK{msg_ids} from the client, on the owner loop. Unknown or repeated ids are ignored. */
  public void onAck(Session s, Ack ack) {
    if (ack.getMsgIdsCount() > MAX_ACK_IDS) {
      throw new ProtocolException(CloseCode.BAD_FRAME, "too many ack ids");
    }
    for (String id : ack.getMsgIdsList()) {
      PendingEntry e = s.ackPending(id);
      if (e != null) {
        e.answer(AckItem.Status.STATUS_ACKED);
      }
    }
  }
}
