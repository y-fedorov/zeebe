/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.broker;

import io.camunda.zeebe.client.ZeebeClient;
import org.junit.jupiter.api.Test;

final class NiceTest {

  @Test
  void shouldRunInstance() {
    System.out.println();

    try (final var zeebeClient = ZeebeClient.newClientBuilder().usePlaintext().build()) {
      System.out.println("Created client");
      final var deploymentEvent =
          zeebeClient
              .newDeployResourceCommand()
              .addResourceFromClasspath("one-task-one-timer.bpmn")
              .send()
              .join();

      System.out.println("Deployed process " + deploymentEvent);
      Loggers.SYSTEM_LOGGER.debug("Deployed process {}", deploymentEvent);
      zeebeClient
          .newWorker()
          .jobType("benchmarkTask")
          .handler(
              (c, job) -> {
                Loggers.SYSTEM_LOGGER.debug("Complete job");
                c.newCompleteCommand(job).send();
              })
          .open();

      Loggers.SYSTEM_LOGGER.debug("Opened worker?");

      final var process = deploymentEvent.getProcesses().get(0);
      while (true) {
        try {
          int count = 0;
          final var startTime = System.currentTimeMillis();
          boolean hasReachedTime;
          long timeDiff;
          final var frequence = 25;
          do {
            final var processInstanceEvent =
                zeebeClient
                    .newCreateInstanceCommand()
                    .processDefinitionKey(process.getProcessDefinitionKey())
                    .send()
                    .join();

            Loggers.SYSTEM_LOGGER.debug("Created instance {}", processInstanceEvent);
            count++;
            timeDiff = (System.currentTimeMillis() - startTime);
            hasReachedTime = timeDiff >= 1000;
          } while (count >= frequence || hasReachedTime);
          timeDiff = 1000 - timeDiff;
          if (timeDiff > 0) {
            Loggers.SYSTEM_LOGGER.debug("Sleep for {}", timeDiff);
            Thread.sleep(timeDiff);
          }
        } catch (final Exception ex) {
          Loggers.SYSTEM_LOGGER.error("err", ex);
          // just do it
        }
      }
    }
  }
}
