/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.otel;

import io.camunda.zeebe.protocol.otel.MessageHeaderEncoder;
import io.camunda.zeebe.protocol.otel.SpanContextEncoder;
import io.camunda.zeebe.protocol.otel.SpanContextEncoder.TraceStateEncoder;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.opentelemetry.api.internal.OtelEncodingUtils;
import io.opentelemetry.api.trace.SpanContext;
import java.nio.charset.StandardCharsets;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;

public final class SpanContextWriter implements BufferWriter {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final SpanContextEncoder bodyEncoder = new SpanContextEncoder();

  private SpanContext context;
  private int length = headerEncoder.encodedLength() + bodyEncoder.sbeBlockLength();

  public SpanContextWriter context(final SpanContext context) {
    this.context = context;
    length =
        headerEncoder.encodedLength()
            + bodyEncoder.sbeBlockLength()
            + TraceStateEncoder.sbeHeaderSize()
            + computeTraceStateLength();

    return this;
  }

  public SpanContext context() {
    return context;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    bodyEncoder
        .wrapAndApplyHeader(buffer, offset, headerEncoder)
        .putTraceId(context.getTraceIdBytes(), 0)
        .spanId(OtelEncodingUtils.longFromBase16String(context.getSpanId(), 0))
        .traceFlags(context.getTraceFlags().asByte());

    final var traceState = context.getTraceState();
    final var stateEncoder = bodyEncoder.traceStateCount(traceState.size());
    traceState.forEach((key, value) -> stateEncoder.key(key).value(value).next());
  }

  private int computeTraceStateLength() {
    if (context == null || context.getTraceState() == null || context.getTraceState().isEmpty()) {
      return 0;
    }

    final var state = context.getTraceState();
    final var length = new MutableInteger(0);
    state.forEach((key, value) -> computeTraceStateEntryLength(length, key, value));

    return length.get();
  }

  private void computeTraceStateEntryLength(
      final MutableInteger length, final String key, final String value) {
    final int keyLength = key.getBytes(StandardCharsets.US_ASCII).length;
    final var valueLength = value.getBytes(StandardCharsets.US_ASCII).length;
    length.addAndGet(
        TraceStateEncoder.sbeBlockLength()
            + TraceStateEncoder.keyHeaderLength()
            + keyLength
            + TraceStateEncoder.valueHeaderLength()
            + valueLength);
  }
}
