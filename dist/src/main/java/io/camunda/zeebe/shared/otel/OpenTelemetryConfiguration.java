/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.shared.otel;

import com.google.cloud.opentelemetry.trace.TraceExporter;
import io.camunda.zeebe.broker.Loggers;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.IOException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnCloudPlatform;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.cloud.CloudPlatform;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration(proxyBeanMethods = false)
public final class OpenTelemetryConfiguration {

  @Bean
  @Primary
  @ConditionalOnCloudPlatform(CloudPlatform.KUBERNETES)
  public OpenTelemetrySdk gcpOpenTelemetrySdk() throws IOException {
    Loggers.SYSTEM_LOGGER.error("Using Cloud Trace OTEL exporter");
    final var traceExporter = TraceExporter.createWithDefaultConfiguration();
    final var spanProcessor = BatchSpanProcessor.builder(traceExporter).build();
    return AutoConfiguredOpenTelemetrySdk.builder()
        .setResultAsGlobal(true)
        .registerShutdownHook(true)
        .addTracerProviderCustomizer(
            (builder, properties) -> builder.addSpanProcessor(spanProcessor))
        .build()
        .getOpenTelemetrySdk();
  }

  @Bean
  @ConditionalOnMissingBean
  public OpenTelemetrySdk openTelemetrySdk() {
    Loggers.SYSTEM_LOGGER.error("Using default OTLP exporter");
    return AutoConfiguredOpenTelemetrySdk.builder()
        .setResultAsGlobal(true)
        .registerShutdownHook(true)
        .build()
        .getOpenTelemetrySdk();
  }
}
