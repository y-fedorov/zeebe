/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.lifecycle;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.camunda.zeebe.scheduler.testing.ControlledActorSchedulerRule;
import org.junit.Rule;
import org.junit.Test;

public final class ActorSuspendTest {
  @Rule
  public final ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  @Test
  public void shouldSuspendActorAndNotExecuteSubmittedJobs() throws Exception {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.run(before);
            actor.suspend();
            actor.run(after);
          }
        };

    // when
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // then
    verify(before, never()).run();
    verify(after, never()).run();
  }


  @Test
  public void shouldResumeActorAndExecuteSubmittedJobs() throws Exception {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.run(before);
            actor.suspend();
            actor.run(after);
          }
        };
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();


    // when
    actor.control().resume();
    schedulerRule.workUntilDone();

    // then
    verify(before, times(1)).run();
    verify(after, times(1)).run();
  }

  @Test
  public void shouldSuspendActorAndNotExecuteFurtherSubmittedJobs() throws Exception {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor();
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // when
    actor.control().submit(before);
    actor.control().submit(() -> actor.control().suspend());
    actor.control().submit(after);
    schedulerRule.workUntilDone();

    // then
    verify(before, times(1)).run();
    verify(after, never()).run();
  }

}
