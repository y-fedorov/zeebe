/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.util.sched.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.util.sched.Actor;
import org.junit.Test;

public final class ActorExampleTest {

  @Test
  public void shouldAddJobToQueue() {
    // given
    final var fakeActor = new FakeActor();

    // when
    fakeActor.someMagicActorMethod();

    // then
    final var currentJobs = fakeActor.getCurrentJobs();
    assertThat(currentJobs).hasSize(1);
    assertThat(currentJobs.get(0).toString()).contains("FakeActor");
    assertThat(fakeActor.runs).isEqualTo(0);
  }

  @Test
  public void shouldRunJob() {
    // given
    final var fakeActor = new FakeActor();
    fakeActor.someMagicActorMethod();

    // when
    fakeActor.executeNext();

    // then
    assertThat(fakeActor.runs).isEqualTo(1);
  }

  public class FakeActor extends Actor {
    public int runs = 0;

    private void internalDo() {
      runs++;
    }

    public void someMagicActorMethod() {
      executionContext.submit(this::internalDo);
    }
  }
}
