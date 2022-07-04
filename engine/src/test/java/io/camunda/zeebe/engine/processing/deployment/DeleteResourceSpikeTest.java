/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.deployment;

import static io.camunda.zeebe.protocol.record.RecordAssert.assertThat;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.engine.util.RecordToWrite;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import io.camunda.zeebe.protocol.record.intent.ResourceDeletionIntent;
import io.camunda.zeebe.protocol.record.value.ResourceDeletionRecordValue;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.camunda.zeebe.util.buffer.BufferUtil;
import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

public class DeleteResourceSpikeTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule public final TestWatcher watcher = new RecordingExporterTestWatcher();

  @Test
  public void deleteResourceTest() throws InterruptedException {
    final String processId = "processId";
    final var process =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .serviceTask("task", s -> s.zeebeJobType("type"))
            .endEvent()
            .done();
    ENGINE.deployment().withXmlResource(process).deploy();
    //    ENGINE.processInstance().ofBpmnProcessId(processId).create();

    final ResourceDeletionRecordValue value =
        new ResourceDeletionRecord().setVersion(1).setBpmnProcessId(processId);

    ENGINE.writeRecords(
        RecordToWrite.command().resourceDeletion(ResourceDeletionIntent.DELETE, value).key(1L));

    assertThat(
            RecordingExporter.records()
                .onlyEvents()
                .filter(x -> x.getIntent() == ResourceDeletionIntent.DELETED)
                .getFirst())
        .isNotNull();

    assertThat(
            RecordingExporter.processRecords()
                .withBpmnProcessId(processId)
                .withIntent(ProcessIntent.DELETED)
                .getFirst())
        .isNotNull();

    Assertions.assertThat(
            ENGINE
                .getZeebeState()
                .getProcessState()
                .getProcessByProcessIdAndVersion(BufferUtil.wrapString(processId), 1))
        .isNull();
  }

  @Test
  public void deleteAndRedeployResourceTest() {
    final String processId = "processId";
    final var process =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .serviceTask("task", s -> s.zeebeJobType("type"))
            .endEvent()
            .done();
    ENGINE.deployment().withXmlResource(process).deploy();

    final ResourceDeletionRecordValue value =
        new ResourceDeletionRecord().setVersion(1).setBpmnProcessId(processId);

    ENGINE.writeRecords(
        RecordToWrite.command().resourceDeletion(ResourceDeletionIntent.DELETE, value).key(1L));

    assertThat(
            RecordingExporter.records()
                .onlyEvents()
                .filter(x -> x.getIntent() == ResourceDeletionIntent.DELETED)
                .getFirst())
        .isNotNull();

    assertThat(
            RecordingExporter.processRecords()
                .withBpmnProcessId(processId)
                .withIntent(ProcessIntent.DELETED)
                .getFirst())
        .isNotNull();

    Assertions.assertThat(
            ENGINE
                .getZeebeState()
                .getProcessState()
                .getProcessByProcessIdAndVersion(BufferUtil.wrapString(processId), 1))
        .isNull();

    ENGINE.deployment().withXmlResource(process).deploy();

    Assertions.assertThat(
            ENGINE
                .getZeebeState()
                .getProcessState()
                .getProcessByProcessIdAndVersion(BufferUtil.wrapString(processId), 2))
        .isNotNull();
  }
}
