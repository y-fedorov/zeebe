/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker.transport;

import io.camunda.zeebe.protocol.record.ValueType;
import java.util.Set;

public abstract class RequestReaderException extends RuntimeException {

  public static final class InvalidTemplateException extends RequestReaderException {
    final int expectedTemplate;
    final int actualTemplate;

    public InvalidTemplateException(final int expectedTemplate, final int actualTemplate) {
      this.expectedTemplate = expectedTemplate;
      this.actualTemplate = actualTemplate;
    }
  }

  public static final class InvalidValueTypeException extends RequestReaderException {
    final Set<ValueType> expectedTypes;
    final ValueType actualType;

    public InvalidValueTypeException(
        final Set<ValueType> expectedTypes, final ValueType actualType) {
      this.expectedTypes = expectedTypes;
      this.actualType = actualType;
    }
  }
}
