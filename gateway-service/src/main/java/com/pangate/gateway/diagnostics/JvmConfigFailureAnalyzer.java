package com.pangate.gateway.diagnostics;

import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

public final class JvmConfigFailureAnalyzer extends AbstractFailureAnalyzer<JvmConfigException> {

  @Override
  protected FailureAnalysis analyze(Throwable root, JvmConfigException cause) {
    String flag = "-XX:" + cause.flag();
    if ("MaxDirectMemorySize".equals(cause.flag())) {
      return new FailureAnalysis(flag + " is not set; the JVM default equals the max heap (50% of the container), "
          + "so heap plus direct memory could take the whole container",
          "With profile prod set it in JAVA_TOOL_OPTIONS (25% of container memory), e.g. "
              + "JAVA_TOOL_OPTIONS=" + flag + "=2g", cause);
    }
    return new FailureAnalysis(flag + " is not set but the active profile requires it",
        "Set " + flag + " in JAVA_TOOL_OPTIONS", cause);
  }
}
