/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler.lifecycle;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.camunda.zeebe.scheduler.testing.ControlledActorSchedulerRule;
import io.camunda.zeebe.util.Loggers;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;

public final class ActorSuspendTest {
  @Rule
  public final ControlledActorSchedulerRule schedulerRule = new ControlledActorSchedulerRule();

  @Test
  public void shouldSuspendActorAndNotExecuteSubmittedJobs() {
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
  public void shouldSuspendDelayedJob() {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.runDelayed(Duration.ZERO, before);
            actor.suspend();
            actor.runDelayed(Duration.ZERO, after);
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
  public void shouldResumeWithDelayedJob() {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.runDelayed(Duration.ZERO, before);
            actor.suspend();
            actor.runDelayed(Duration.ZERO, after);
          }
        };
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // when
    actor.control().resume();
    schedulerRule.workUntilDone();

    // then
    verify(before, timeout(10 * 1000)).run();
    verify(after, never()).run(); // was rejected
  }

  @Test
  public void shouldClose() throws InterruptedException {
    // given
    final var countDownLatch = new CountDownLatch(1);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.runDelayed(Duration.ofSeconds(1), () -> {
                  Loggers.ACTOR_LOGGER.error("delayed");
                  countDownLatch.countDown();
                });
            actor.close();
          }

          @Override
          public void onActorClosing() {
            Loggers.ACTOR_LOGGER.error("closing");
          }

          @Override
          public void onActorClosed() {
            Loggers.ACTOR_LOGGER.error("closed");
          }

          @Override
          public void onActorCloseRequested() {

            Loggers.ACTOR_LOGGER.error("onActorCloseRequested");
            try {
              Thread.sleep(1);
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        };
    schedulerRule.submitActor(actor);


    // when
    schedulerRule.workUntilDone();

    // then
    countDownLatch.await(10, TimeUnit.SECONDS);
    assertThat(actor.isActorClosed()).isTrue();
  }


  @Test
  public void shouldResumeActorAndExecuteSubmittedJobs()  {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.run(before);
            actor.suspend();
            actor.run(after); // is rejected
          }
        };
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // when
    actor.control().resume();
    schedulerRule.workUntilDone();

    // then
    verify(before, timeout(10 * 1000)).run();
    verify(after, never()).run(); // was rejected
  }

  @Test
  public void shouldEnqueueSuspendWithRun()  {
    // given
    final Runnable before = mock(Runnable.class);
    final Runnable after = mock(Runnable.class);
    final LifecycleRecordingActor actor =
        new LifecycleRecordingActor() {
          @Override
          public void onActorStarted() {
            actor.run(before);
            actor.run(actor::suspend);
            actor.run(after);
          }
        };
    schedulerRule.submitActor(actor);
    schedulerRule.workUntilDone();

    // when
    actor.control().resume();
    schedulerRule.workUntilDone();

    // then
    verify(before, timeout(10 * 1000)).run();
    verify(after, timeout(10 * 1000)).run();
  }

  @Test
  public void shouldSuspendActorAndNotExecuteFurtherSubmittedJobs() {
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
