package com.pangate.chat.core;

import java.util.Set;

/** Devices that have a conversation open on screen, as {@code <userId>/<deviceId>} (CH3 spec §5, §6.3). */
@FunctionalInterface
public interface OpenConversations {

  Set<String> devicesWithOpen(String convId);

  OpenConversations NONE = conv -> Set.of();
}
