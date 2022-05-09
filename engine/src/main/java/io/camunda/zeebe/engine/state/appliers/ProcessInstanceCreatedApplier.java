/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceCreationIntent;

public class ProcessInstanceCreatedApplier
    implements TypedEventApplier<ProcessInstanceCreationIntent, ProcessInstanceCreationRecord> {

  private final MutableElementInstanceState elementInstanceState;

  public ProcessInstanceCreatedApplier(final MutableElementInstanceState elementInstanceState) {

    this.elementInstanceState = elementInstanceState;
  }

  @Override
  public void applyState(final long key, final ProcessInstanceCreationRecord value) {
    final var flowScopeInstance = elementInstanceState.getInstance(value.getProcessInstanceKey());
    flowScopeInstance.incrementActiveSequenceFlows();
    elementInstanceState.updateInstance(flowScopeInstance);
  }
}
