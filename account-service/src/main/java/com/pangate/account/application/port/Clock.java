package com.pangate.account.application.port;

import java.time.Instant;

public interface Clock {

  Clock SYSTEM = Instant::now;

  Instant now();
}
