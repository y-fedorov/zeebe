/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.stream.api.scheduling;

import java.time.Duration;

public interface ProcessingScheduleService extends SimpleProcessingScheduleService {

  /**
   * Schedule a task to execute at a fixed rate. After an initial delay, the task is executed. Once
   * the task is executed, it is rescheduled with the same delay again.
   *
   * <p>The execution of the scheduled task is running asynchron/concurrent to other scheduled
   * tasks. Other methods will guarantee ordering of scheduled tasks and running always in same
   * thread, these guarantees don't apply to this method.
   *
   * <p>Note that time-traveling in tests only affects the delay of the currently scheduled next
   * task and not any of the iterations after. This is because the next task is scheduled with the
   * delay counted from the new time (i.e. the time after time traveling + task execution duration +
   * delay duration = scheduled time of the next task).
   *
   * @param delay The delay to wait initially and between each run
   * @param task The task to execute at the fixed rate
   */
  void runAtFixedRateAsync(final Duration delay, final Task task);
}
