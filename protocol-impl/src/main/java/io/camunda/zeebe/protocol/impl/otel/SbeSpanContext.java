/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.otel;

import io.camunda.zeebe.util.buffer.BufferReader;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** TODO: this can throw so many NPEs if we haven't read a context or set a writer context */
public final class SbeSpanContext implements SpanContext, BufferWriter, BufferReader {
  private final SpanContextReader reader = new SpanContextReader();
  private final SpanContextWriter writer = new SpanContextWriter();

  public SbeSpanContext wrap(final SpanContext context) {
    writer.context(context);
    return this;
  }

  public SbeSpanContext reset() {
    writer.context(null);
    return this;
  }

  public boolean hasContext() {
    return writer.context() != null;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    reader.wrap(buffer, offset, length);
    writer.context(reader.context());
  }

  @Override
  public int getLength() {
    return writer.getLength();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    writer.write(buffer, offset);
  }

  @Override
  public String getTraceId() {
    return writer.context().getTraceId();
  }

  @Override
  public byte[] getTraceIdBytes() {
    return writer.context().getTraceIdBytes();
  }

  @Override
  public String getSpanId() {
    return writer.context().getSpanId();
  }

  @Override
  public byte[] getSpanIdBytes() {
    return writer.context().getSpanIdBytes();
  }

  @Override
  public boolean isSampled() {
    return writer.context().isSampled();
  }

  @Override
  public TraceFlags getTraceFlags() {
    return writer.context().getTraceFlags();
  }

  @Override
  public TraceState getTraceState() {
    return writer.context().getTraceState();
  }

  @Override
  public boolean isValid() {
    return writer.context().isValid();
  }

  @Override
  public boolean isRemote() {
    return writer.context().isRemote();
  }

  public SpanContext copy() {
    if (isRemote()) {
      return SpanContext.createFromRemoteParent(
          getTraceId(), getSpanId(), getTraceFlags(), getTraceState());
    } else {
      return SpanContext.create(getTraceId(), getSpanId(), getTraceFlags(), getTraceState());
    }
  }
}
