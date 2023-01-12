/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.impl.records;

import io.camunda.zeebe.logstreams.log.LogAppendEntry;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRelated;
import io.camunda.zeebe.stream.api.records.ExceededBatchRecordSizeException;
import io.camunda.zeebe.stream.api.records.ImmutableRecordBatch;
import io.camunda.zeebe.stream.api.records.MutableRecordBatch;
import io.camunda.zeebe.stream.api.records.RecordBatchSizePredicate;
import io.camunda.zeebe.util.Either;
import io.camunda.zeebe.util.VersionUtil;
import io.camunda.zeebe.util.buffer.BufferWriter;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public final class RecordBatch implements MutableRecordBatch {

  final List<RecordBatchEntry> recordBatchEntries = new ArrayList<>();
  private int batchSize;
  private final RecordBatchSizePredicate recordBatchSizePredicate;
  private final Tracer tracer =
      GlobalOpenTelemetry.getTracer("io.camunda.zeebe.broker.stream", VersionUtil.getVersion());

  public RecordBatch(final RecordBatchSizePredicate recordBatchSizePredicate) {
    this.recordBatchSizePredicate = recordBatchSizePredicate;
  }

  public static ImmutableRecordBatch empty() {
    return new RecordBatch((c, s) -> false);
  }

  @Override
  public Either<RuntimeException, Void> appendRecord(
      final long key,
      final int sourceIndex,
      final RecordType recordType,
      final Intent intent,
      final RejectionType rejectionType,
      final String rejectionReason,
      final ValueType valueType,
      final BufferWriter valueWriter,
      final SpanContext spanContext) {
    final Span span;
    if (recordType == RecordType.COMMAND) {
      final var builder = tracer.spanBuilder("appendRecord").setSpanKind(SpanKind.PRODUCER);
      if (spanContext != null && spanContext.isValid() && spanContext.isSampled()) {
        builder.addLink(spanContext);
      }

      if (valueWriter instanceof ProcessInstanceRecord p) {
        builder.setAttribute("bpmnElementType", p.getBpmnElementType().name());
      }

      if (valueWriter instanceof ProcessInstanceRelated r) {
        builder.setAttribute("processInstanceKey", r.getProcessInstanceKey());
      }

      span =
          builder
              .setAttribute("intent", intent.name())
              .setAttribute("valueType", valueType.name())
              .startSpan();
    } else {
      span = Span.getInvalid();
    }

    try {
      final var recordBatchEntry =
          RecordBatchEntry.createEntry(
              key,
              sourceIndex,
              recordType,
              intent,
              rejectionType,
              rejectionReason,
              valueType,
              valueWriter);
      final var entryLength = recordBatchEntry.getLength();
      if (span.getSpanContext().isValid()) {
        recordBatchEntry.recordMetadata().spanContext().wrap(span.getSpanContext());
      }

      if (!recordBatchSizePredicate.test(recordBatchEntries.size() + 1, batchSize + entryLength)) {
        return Either.left(
            new ExceededBatchRecordSizeException(
                recordBatchEntry, entryLength, recordBatchEntries.size(), batchSize));
      }

      recordBatchEntries.add(recordBatchEntry);
      batchSize += entryLength;
      return Either.right(null);
    } finally {
      span.end();
    }
  }

  @Override
  public boolean canAppendRecordOfLength(final int recordLength) {
    return recordBatchSizePredicate.test(recordBatchEntries.size() + 1, batchSize + recordLength);
  }

  public int getBatchSize() {
    return batchSize;
  }

  @Override
  public Iterator<RecordBatchEntry> iterator() {
    return recordBatchEntries.iterator();
  }

  @Override
  public void forEach(final Consumer<? super RecordBatchEntry> action) {
    recordBatchEntries.forEach(action);
  }

  @Override
  public Spliterator<RecordBatchEntry> spliterator() {
    return recordBatchEntries.spliterator();
  }

  @Override
  public List<LogAppendEntry> entries() {
    return Collections.unmodifiableList(recordBatchEntries);
  }
}
