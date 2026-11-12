package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatSend;

/** One send as Ingest sees it: the sender without its {@code chat:} prefix; {@code replyTo} is empty or {@code http:<pod>}. */
public record SendInput(String senderId, String senderDeviceId, ChatSend send, String replyTo) {}
