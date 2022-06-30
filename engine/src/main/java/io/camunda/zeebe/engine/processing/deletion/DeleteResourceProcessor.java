/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deletion;

import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import io.camunda.zeebe.protocol.record.intent.ResourceDeletionIntent;
import io.camunda.zeebe.util.buffer.BufferUtil;
import java.util.function.Consumer;

public class DeleteResourceProcessor implements TypedRecordProcessor<ResourceDeletionRecord> {

  private final StateWriter stateWriter;
  private final ProcessState processState;

  public DeleteResourceProcessor(final Writers writers, final ZeebeState zeebeState) {
    stateWriter = writers.state();
    processState = zeebeState.getProcessState();
  }

  @Override
  public void processRecord(
      final TypedRecord<ResourceDeletionRecord> command,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {
    System.out.println("Command received! Deleting resource, *robot noises* ...");

    // find the process
    final var value = command.getValue();
    final var process =
        processState.getProcessByProcessIdAndVersion(
            BufferUtil.wrapString(value.getBpmnProcessId()), value.getVersion());

    // write event to delete the definition
    // todo: fill in additional info on value
    stateWriter.appendFollowUpEvent(process.getKey(), ResourceDeletionIntent.DELETED, value);

    // deletes the process from state
    final var processRecord = new ProcessRecord();
    processRecord.setKey(process.getKey());
    processRecord.setBpmnProcessId(value.getBpmnProcessId());
    processRecord.setVersion(value.getVersion());
    processRecord.setResourceName(process.getResourceName());
    processRecord.setResource(process.getResource());
    processRecord.setChecksum(BufferUtil.wrapString(""));
    stateWriter.appendFollowUpEvent(process.getKey(), ProcessIntent.DELETED, processRecord);
  }
}
