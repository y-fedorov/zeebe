/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class ActorSchedulerTest {

  @Test
  void shouldRunOtherActor() throws InterruptedException {
    // given
    final var busy = new Actor() {
      @Override
      protected void onActorStarted() {
        actor.run(this::myWork);
      }

      private void myWork() {
        System.out.println("I'm busy I have work to do");
        actor.run(this::myWork);
      }


    };
    final CountDownLatch latch = new CountDownLatch(1);
    final var yolo = new Actor() {
      @Override
      protected void onActorStarted() {
        actor.run(this::myWork);
      }

      private void myWork() {
        System.out.println("Yolo I would also do someee stuff");
        latch.countDown();
        actor.run(this::myWork);
      }
    };
    final var scheduler = ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(1).build();
    scheduler.start();

    // when
    scheduler.submitActor(busy);
    scheduler.submitActor(yolo);

    // then
    assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
  }


  @Test
  void shouldThrowIllegalStateExceptionWhenTaskIsSubmittedBeforeActorSchedulerIsNotRunning() {
    // given
    final var testActor = new TestActor();
    final var sut = ActorScheduler.newActorScheduler().build();

    // when + then
    assertThatThrownBy(() -> sut.submitActor(testActor)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> sut.submitActor(testActor, SchedulingHints.cpuBound()))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void shouldThrowIllegalStateExceptionWhenTaskIsSubmittedAfterActorSchedulerIsStopped() {
    // given
    final var testActor = new TestActor();
    final var sut = ActorScheduler.newActorScheduler().build();

    sut.start();
    final var stopFuture = sut.stop();

    await().until(stopFuture::isDone);

    // when + then
    assertThatThrownBy(() -> sut.submitActor(testActor)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> sut.submitActor(testActor, SchedulingHints.cpuBound()))
        .isInstanceOf(IllegalStateException.class);
  }

  private static final class TestActor extends Actor {}
}
