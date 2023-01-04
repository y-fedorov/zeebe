/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.otel;

import io.camunda.zeebe.protocol.otel.MessageHeaderDecoder;
import io.camunda.zeebe.protocol.otel.SpanContextDecoder;
import io.camunda.zeebe.util.buffer.BufferReader;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import org.agrona.DirectBuffer;

public final class SpanContextReader implements BufferReader {
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final SpanContextDecoder bodyDecoder = new SpanContextDecoder();
  private SpanContext context;

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    bodyDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

    final var traceIdBytes = new byte[SpanContextDecoder.traceIdLength()];
    bodyDecoder.getTraceId(traceIdBytes, 0);

    final var traceId = TraceId.fromBytes(traceIdBytes);
    final var spanId = SpanId.fromLong(bodyDecoder.spanId());
    final var traceFlags = TraceFlags.fromByte(bodyDecoder.traceFlags());
    final var traceState = decodeTraceState();

    // TODO: we assume that deserialization means we're tracing a remote trace, which is not
    // necessarily true, but for now it's simpler to assume this
    context = SpanContext.createFromRemoteParent(traceId, spanId, traceFlags, traceState);
  }

  public SpanContext context() {
    return context;
  }

  private TraceState decodeTraceState() {
    final var builder = TraceState.builder();
    var stateDecoder = bodyDecoder.traceState();
    while (stateDecoder.hasNext()) {
      builder.put(stateDecoder.key(), stateDecoder.value());
      stateDecoder = stateDecoder.next();
    }

    return builder.build();
  }
}
