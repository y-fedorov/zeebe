/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.otel;

import io.camunda.zeebe.util.VersionUtil;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;

public final class OpenTelemetryProvider {
  private static final OpenTelemetry OTEL = GlobalOpenTelemetry.get();
  private static final Tracer TRACER =
      OTEL.getTracer("io.camunda.zeebe.gateway", VersionUtil.getVersion());

  private OpenTelemetryProvider() {}

  public static OpenTelemetry sdk() {
    return OTEL;
  }

  public static Tracer tracer() {
    return TRACER;
  }
}
