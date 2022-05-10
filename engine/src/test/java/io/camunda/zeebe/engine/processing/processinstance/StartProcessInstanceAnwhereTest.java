/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.processinstance;

import io.camunda.zeebe.engine.util.EngineRule;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.intent.TimerIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.time.Duration;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class StartProcessInstanceAnwhereTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Test
  public void testStartAnywhereStartEnd() {
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("process").startEvent().endEvent("end").done())
        .deploy();

    final long key =
        ENGINE.processInstance().ofBpmnProcessId("process").withElementId("end").create();

    Assertions.assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(key)
                .limitToProcessInstanceCompleted())
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            Tuple.tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            Tuple.tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED))
        .doesNotContain(
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void testStartAnywhereEventSubProcess() {
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("process")
                .eventSubProcess(
                    "event",
                    b ->
                        b.startEvent(
                                "eventStart",
                                c ->
                                    c.message(
                                        d ->
                                            d.name("message")
                                                .zeebeCorrelationKeyExpression(
                                                    "\"correlationKey\"")))
                            .endEvent()
                            .done())
                .startEvent()
                .serviceTask("task", b -> b.zeebeJobType("type"))
                .endEvent("end")
                .done())
        .deploy();

    final long key =
        ENGINE.processInstance().ofBpmnProcessId("process").withElementId("task").create();

    ENGINE.job().withType("type").ofInstance(key).complete();

    Assertions.assertThat(
            RecordingExporter.processInstanceRecords()
                .onlyEvents()
                .withProcessInstanceKey(key)
                .limitToProcessInstanceCompleted())
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED))
        .doesNotContain(
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED));

    Assertions.assertThat(
            RecordingExporter.messageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .withCorrelationKey("correlationKey")
                .withIntent(MessageSubscriptionIntent.CREATED)
                .findFirst())
        .isNotEmpty();

    Assertions.assertThat(
            RecordingExporter.processMessageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .findFirst())
        .isNotEmpty();
  }

  @Test
  public void testStartAnywhereEventSubProcessSendMessage() {
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("process")
                .eventSubProcess(
                    "event",
                    b ->
                        b.startEvent(
                                "eventStart",
                                c ->
                                    c.message(
                                        d ->
                                            d.name("message")
                                                .zeebeCorrelationKeyExpression(
                                                    "\"correlationKey\"")))
                            .endEvent()
                            .done())
                .startEvent()
                .serviceTask("task", b -> b.zeebeJobType("type"))
                .endEvent("end")
                .done())
        .deploy();

    final long key =
        ENGINE.processInstance().ofBpmnProcessId("process").withElementId("task").create();

    ENGINE.message().withName("message").withCorrelationKey("correlationKey").publish();

    Assertions.assertThat(
            RecordingExporter.processInstanceRecords()
                .onlyEvents()
                .withProcessInstanceKey(key)
                .limitToProcessInstanceCompleted())
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(
                BpmnElementType.EVENT_SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.EVENT_SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(
                BpmnElementType.EVENT_SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.EVENT_SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED))
        .doesNotContain(
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED));

    Assertions.assertThat(
            RecordingExporter.messageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .withCorrelationKey("correlationKey")
                .withIntent(MessageSubscriptionIntent.CREATED)
                .findFirst())
        .isNotEmpty();

    Assertions.assertThat(
            RecordingExporter.processMessageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .findFirst())
        .isNotEmpty();
  }

  @Test
  public void testStartAnywhereBoundaryEvent() {
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("process")
                .eventSubProcess(
                    "event",
                    b ->
                        b.startEvent(
                                "eventStart",
                                c ->
                                    c.message(
                                        d ->
                                            d.name("message")
                                                .zeebeCorrelationKeyExpression("correlationKey")))
                            .endEvent()
                            .done())
                .startEvent()
                .serviceTask("task", b -> b.zeebeJobType("type"))
                .boundaryEvent("boundary", b -> b.timerWithDuration("PT1M").endEvent())
                .moveToNode("task")
                .endEvent("end")
                .done())
        .deploy();

    final long key =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("process")
            .withVariables(Map.of("correlationKey", "nico"))
            .withElementId("task")
            .create();

    Assertions.assertThat(
            RecordingExporter.timerRecords(TimerIntent.CREATED)
                .withProcessInstanceKey(key)
                .findFirst())
        .isNotEmpty();

    //    ENGINE.message().withName("message").withCorrelationKey("nico").publish();
    ENGINE.increaseTime(Duration.ofMinutes(2));

    Assertions.assertThat(
            RecordingExporter.processInstanceRecords()
                .onlyEvents()
                .withProcessInstanceKey(key)
                .limitToProcessInstanceCompleted())
        .extracting(record -> record.getValue().getBpmnElementType(), Record::getIntent)
        .containsSubsequence(
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            Tuple.tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            Tuple.tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED))
        .doesNotContain(
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETING),
            Tuple.tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED));

    Assertions.assertThat(
            RecordingExporter.messageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .withCorrelationKey("nico")
                .withIntent(MessageSubscriptionIntent.CREATED)
                .findFirst())
        .isNotEmpty();

    Assertions.assertThat(
            RecordingExporter.processMessageSubscriptionRecords()
                .withProcessInstanceKey(key)
                .withMessageName("message")
                .findFirst())
        .isNotEmpty();
  }
}
