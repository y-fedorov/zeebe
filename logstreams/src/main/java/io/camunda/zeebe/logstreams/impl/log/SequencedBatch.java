/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.logstreams.impl.log;

import io.camunda.zeebe.logstreams.impl.serializer.SequencedBatchSerializer;
import io.camunda.zeebe.logstreams.log.LogAppendEntry;
import io.camunda.zeebe.protocol.impl.otel.SbeSpanContext;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.util.SpanContextProvider;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.opentelemetry.api.trace.SpanContext;
import java.util.List;
import org.agrona.MutableDirectBuffer;

public record SequencedBatch(
    long timestamp, long firstPosition, long sourcePosition, List<LogAppendEntry> entries)
    implements BufferWriter, SpanContextProvider {

  @Override
  public List<SpanContext> spanContexts() {
    return entries.stream()
        .map(LogAppendEntry::recordMetadata)
        .map(RecordMetadata::spanContext)
        .filter(SbeSpanContext::hasContext)
        .map(SpanContext.class::cast)
        .toList();
  }

  @Override
  public int getLength() {
    return SequencedBatchSerializer.calculateBatchSize(this);
  }

  @Override
  public void write(final MutableDirectBuffer buffer, final int offset) {
    SequencedBatchSerializer.serializeBatch(buffer, offset, this);
  }
}
