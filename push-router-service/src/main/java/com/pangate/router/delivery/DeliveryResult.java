package com.pangate.router.delivery;

/** How a record ended: delivered (or handed to every node), sent offline, or dropped (EPHEMERAL / nothing to do). */
public enum DeliveryResult { DELIVERED, OFFLINE, DROPPED }
