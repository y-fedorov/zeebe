/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.atomix.cluster.messaging.impl;

import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ExecutorDecorator implements Executor {

  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Executor delegate;

  public ExecutorDecorator(final Executor delegate) {
    this.delegate = delegate;
  }

  @Override
  public void execute(final Runnable runnable) {
    try {
      delegate.execute(runnable);
    } catch (Exception ex) {
      log.error("Error on executing runnable {}", runnable, ex);
      throw ex;
    }
  }
}
