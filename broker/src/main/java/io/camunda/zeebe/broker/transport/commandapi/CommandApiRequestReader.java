/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.transport.commandapi;

import io.camunda.zeebe.broker.transport.AsyncApiRequestHandler.RequestReader;
import io.camunda.zeebe.broker.transport.RequestReaderException;
import io.camunda.zeebe.protocol.impl.otel.SbeSpanContext;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.camunda.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceModificationRecord;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.impl.record.value.signal.SignalRecord;
import io.camunda.zeebe.protocol.impl.record.value.variable.VariableDocumentRecord;
import io.camunda.zeebe.protocol.record.ExecuteCommandRequestDecoder;
import io.camunda.zeebe.protocol.record.MessageHeaderDecoder;
import io.camunda.zeebe.protocol.record.ValueType;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;

public class CommandApiRequestReader implements RequestReader<ExecuteCommandRequestDecoder> {
  static final Map<ValueType, Supplier<UnifiedRecordValue>> RECORDS_BY_TYPE =
      new EnumMap<>(ValueType.class);

  static {
    RECORDS_BY_TYPE.put(ValueType.DEPLOYMENT, DeploymentRecord::new);
    RECORDS_BY_TYPE.put(ValueType.JOB, JobRecord::new);
    RECORDS_BY_TYPE.put(ValueType.PROCESS_INSTANCE, ProcessInstanceRecord::new);
    RECORDS_BY_TYPE.put(ValueType.MESSAGE, MessageRecord::new);
    RECORDS_BY_TYPE.put(ValueType.JOB_BATCH, JobBatchRecord::new);
    RECORDS_BY_TYPE.put(ValueType.INCIDENT, IncidentRecord::new);
    RECORDS_BY_TYPE.put(ValueType.VARIABLE_DOCUMENT, VariableDocumentRecord::new);
    RECORDS_BY_TYPE.put(ValueType.PROCESS_INSTANCE_CREATION, ProcessInstanceCreationRecord::new);
    RECORDS_BY_TYPE.put(
        ValueType.PROCESS_INSTANCE_MODIFICATION, ProcessInstanceModificationRecord::new);
    RECORDS_BY_TYPE.put(ValueType.SIGNAL, SignalRecord::new);
  }

  private UnifiedRecordValue value;
  private final RecordMetadata metadata = new RecordMetadata();
  private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
  private final ExecuteCommandRequestDecoder commandRequestDecoder =
      new ExecuteCommandRequestDecoder();
  private final SbeSpanContext spanContext = new SbeSpanContext();

  @Override
  public void reset() {
    if (value != null) {
      value.reset();
    }

    spanContext.reset();
    metadata.reset();
  }

  @Override
  public ExecuteCommandRequestDecoder getMessageDecoder() {
    return commandRequestDecoder;
  }

  @Override
  public void wrap(final DirectBuffer buffer, final int offset, final int length) {
    try {
      commandRequestDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);
    } catch (final IllegalStateException e) {
      throw new RequestReaderException.InvalidTemplateException(
          ExecuteCommandRequestDecoder.TEMPLATE_ID, messageHeaderDecoder.templateId());
    }

    metadata.protocolVersion(messageHeaderDecoder.version());
    decodeValue(buffer);
    decodeSpanContext(buffer);
  }

  private void decodeSpanContext(final DirectBuffer buffer) {
    final var spanContextLength = commandRequestDecoder.spanContextLength();
    if (spanContextLength > 0) {
      final int spanContextOffset =
          commandRequestDecoder.limit() + ExecuteCommandRequestDecoder.spanContextHeaderLength();
      spanContext.wrap(buffer, spanContextOffset, spanContextLength);
    }
    commandRequestDecoder.skipSpanContext();
  }

  private void decodeValue(final DirectBuffer buffer) {
    final var recordSupplier = RECORDS_BY_TYPE.get(commandRequestDecoder.valueType());
    if (recordSupplier != null) {
      final int valueOffset =
          commandRequestDecoder.limit() + ExecuteCommandRequestDecoder.valueHeaderLength();
      final int valueLength = commandRequestDecoder.valueLength();
      value = recordSupplier.get();
      value.wrap(buffer, valueOffset, valueLength);
    } else {
      throw new RequestReaderException.InvalidValueTypeException(
          RECORDS_BY_TYPE.keySet(), commandRequestDecoder.valueType());
    }
    commandRequestDecoder.skipValue();
  }

  public UnifiedRecordValue value() {
    return value;
  }

  public RecordMetadata metadata() {
    return metadata;
  }
}
