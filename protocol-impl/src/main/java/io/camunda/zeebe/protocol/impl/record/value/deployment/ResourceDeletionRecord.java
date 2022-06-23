/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.protocol.impl.record.value.deployment;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.camunda.zeebe.msgpack.property.IntegerProperty;
import io.camunda.zeebe.msgpack.property.StringProperty;
import io.camunda.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.camunda.zeebe.protocol.record.value.ResourceDeletionRecordValue;

public class ResourceDeletionRecord extends UnifiedRecordValue
    implements ResourceDeletionRecordValue {

  private final StringProperty bpmnProcessIdProp = new StringProperty("bpmnProcessId");
  private final IntegerProperty versionProp = new IntegerProperty("version");

  public ResourceDeletionRecord() {
    declareProperty(bpmnProcessIdProp).declareProperty(versionProp);
  }

  @Override
  public String getBpmnProcessId() {
    return bufferAsString(bpmnProcessIdProp.getValue());
  }

  @Override
  public int getVersion() {
    return versionProp.getValue();
  }

  public ResourceDeletionRecord setVersion(final int version) {
    versionProp.setValue(version);
    return this;
  }

  public ResourceDeletionRecord setBpmnProcessId(final String bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }
}
