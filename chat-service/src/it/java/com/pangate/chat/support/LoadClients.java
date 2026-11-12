package com.pangate.chat.support;

import com.pangate.contracts.v1.ChannelOpen;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.devtools.client.ClientTls;
import com.pangate.devtools.client.GatewayClient;
import com.pangate.devtools.client.Hellos;
import com.pangate.devtools.jwt.JwtMinter;
import com.pangate.devtools.jwt.TokenRequest;
import com.pangate.devtools.keys.DevKeys;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * WebSocket sessions in their own JVM for ChatLoadIT (one process cannot hold 10 000 client and 10 000 server sockets
 * under a 20 000 file limit). Args: devDir issuer count channelId wsPort... Connects {@code count} users, sends
 * {@code chat.channel.open}, prints {@code READY}; then records the latency of every chat.message whose text is
 * {@code w=<epoch µs>}; on stdin {@code RESET} clears, {@code REPORT} prints {@code REPORT n p50 p99 max} (µs).
 */
public final class LoadClients {

  public static void main(String[] args) throws Exception {
    Path dev = Path.of(args[0]);
    JwtMinter minter = new JwtMinter(DevKeys.load(dev.resolve("es256.pem")), args[1]);
    int count = Integer.parseInt(args[2]);
    String channel = args[3];
    List<Integer> ports = new ArrayList<>();
    for (int i = 4; i < args.length; i++) {
      ports.add(Integer.parseInt(args[i]));
    }
    ClientTls tls = ClientTls.trust(dev.resolve("ca.crt"));
    ConcurrentLinkedQueue<Long> lat = new ConcurrentLinkedQueue<>();
    List<GatewayClient> clients = Collections.synchronizedList(new ArrayList<>());
    String run = UUID.randomUUID().toString().substring(0, 6);
    byte[] open = ChannelOpen.newBuilder().setConversationId(channel).build().toByteArray();
    Duration t = Duration.ofSeconds(30);
    try (ExecutorService pool = Executors.newFixedThreadPool(32)) {
      List<Future<?>> fs = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        int n = i;
        fs.add(pool.submit(() -> {
          GatewayClient c = GatewayClient.connect(URI.create("wss://localhost:" + ports.get(n % ports.size())
              + "/v1/ws"), tls, t);
          String jwt = minter.mint(new TokenRequest("chat", "chat:l" + run + "-" + n, "d1", List.of(),
              Duration.ofHours(1)), Instant.now().minusSeconds(1));
          c.hello(Hellos.jwt(jwt, "d1"), t);
          String id = UUID.randomUUID().toString();
          c.send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId(id).putHeaders("type", "cmd")
              .putHeaders("op", "chat.channel.open").setPayload(com.google.protobuf.ByteString.copyFrom(open))
              .build());
          c.await(FrameType.FRAME_TYPE_UPSTREAM_ACK, t);
          c.onPush(f -> {
            if (f.getClass_() == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
              c.ack(f.getMsgId());
            }
            try {
              String text = ChatMessage.parseFrom(f.getPayload()).getText();
              if (text.startsWith("w=")) {
                Instant now = Instant.now();
                long nowUs = now.getEpochSecond() * 1_000_000 + now.getNano() / 1_000;
                lat.add(nowUs - Long.parseLong(text.substring(2)));
              }
            } catch (Exception ignored) {
              // not a chat message
            }
          });
          clients.add(c);
          return null;
        }));
      }
      for (Future<?> f : fs) {
        f.get();
      }
    }
    System.out.println("READY " + clients.size());
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    for (String line; (line = in.readLine()) != null; ) {
      if (line.equals("RESET")) {
        lat.clear();
        System.out.println("OK");
      } else if (line.equals("COUNT")) {
        System.out.println("COUNT " + lat.size());
      } else if (line.equals("REPORT")) {
        List<Long> l = new ArrayList<>(lat);
        Collections.sort(l);
        System.out.println(l.isEmpty() ? "REPORT 0 0 0 0" : "REPORT " + l.size() + " " + l.get(l.size() / 2) + " "
            + l.get((int) Math.min(l.size() - 1, l.size() * 0.99)) + " " + l.getLast());
      } else if (line.equals("EXIT")) {
        break;
      }
    }
    clients.forEach(GatewayClient::abort);
    System.exit(0);
  }
}
