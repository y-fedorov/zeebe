/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.protocol.impl.otel.SbeSpanContext;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

public final class SbeSpanContextTest {
  @Test
  void shouldSerializeSpanContext() {
    // given
    final var original =
        SpanContext.create(
            TraceId.fromLongs(1L, 2L),
            SpanId.fromLong(3L),
            TraceFlags.getSampled(),
            TraceState.builder().put("foo", "bar").put("baz", "boz").build());
    final var contextWriter = new SbeSpanContext();
    final var contextReader = new SbeSpanContext();
    contextWriter.wrap(original);

    // when
    final var buffer = new UnsafeBuffer(new byte[contextWriter.getLength()]);
    contextWriter.write(buffer, 0);
    contextReader.wrap(buffer, 0, contextWriter.getLength());

    // then
    assertThat(contextReader.getTraceId()).isEqualTo(original.getTraceId());
    assertThat(contextReader.getSpanId()).isEqualTo(original.getSpanId());
    assertThat(contextReader.getTraceFlags()).isEqualTo(original.getTraceFlags());
    assertThat(contextReader.getTraceState().asMap())
        .containsExactlyInAnyOrderEntriesOf(Map.of("foo", "bar", "baz", "boz"));
    assertThat(contextReader.isRemote()).isTrue();
  }

  @Test
  void shouldSerializeSpanContextWithNoState() {
    // given
    final var original =
        SpanContext.create(
            TraceId.fromLongs(1L, 2L),
            SpanId.fromLong(3L),
            TraceFlags.getSampled(),
            TraceState.getDefault());
    final var contextWriter = new SbeSpanContext();
    final var contextReader = new SbeSpanContext();
    contextWriter.wrap(original);

    // when
    final var buffer = new UnsafeBuffer(new byte[contextWriter.getLength()]);
    contextWriter.write(buffer, 0);
    contextReader.wrap(buffer, 0, contextWriter.getLength());

    // then
    assertThat(contextReader.getTraceId()).isEqualTo(original.getTraceId());
    assertThat(contextReader.getSpanId()).isEqualTo(original.getSpanId());
    assertThat(contextReader.getTraceFlags()).isEqualTo(original.getTraceFlags());
    assertThat(contextReader.getTraceState().asMap()).isEmpty();
    assertThat(contextReader.isRemote()).isTrue();
  }
}
