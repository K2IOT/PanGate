package com.pangate.gateway.server;

/** Netty transport selection: AUTO picks epoll when available, else NIO. */
public enum Transport {
  AUTO,
  EPOLL,
  NIO
}
