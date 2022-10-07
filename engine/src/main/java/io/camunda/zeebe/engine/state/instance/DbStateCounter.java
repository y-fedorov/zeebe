/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.state.instance;

import io.camunda.zeebe.db.ColumnFamily;
import io.camunda.zeebe.db.TransactionContext;
import io.camunda.zeebe.db.ZeebeDb;
import io.camunda.zeebe.db.impl.DbString;
import io.camunda.zeebe.engine.api.ReadonlyStreamProcessorContext;
import io.camunda.zeebe.engine.api.StreamProcessorLifecycleAware;
import io.camunda.zeebe.engine.state.NextValue;
import io.camunda.zeebe.engine.state.ZbColumnFamilies;
import io.prometheus.client.Gauge;
import java.util.function.BooleanSupplier;

public class DbStateCounter implements StreamProcessorLifecycleAware {
  private static final String NAMESPACE = "zeebe";
  private static final String PARTITION_LABEL = "partition";
  static final Gauge STATE_COUNTER =
      Gauge.build()
          .namespace(NAMESPACE)
          .name("state_counter_total")
          .help("Current counter for column family ")
          .labelNames(PARTITION_LABEL, "columnFamily")
          .register();
  private final long initialValue;
  private final ColumnFamily<DbString, NextValue> nextValueColumnFamily;
  private final DbString nextValueKey;
  private final NextValue nextValue = new NextValue();
  private final String partitionId;
  private final BooleanSupplier metricsEnabled;

  public DbStateCounter(
      final int partitionId,
      final long initialValue,
      final ZeebeDb<ZbColumnFamilies> zeebeDb,
      final TransactionContext transactionContext,
      final ZbColumnFamilies columnFamily,
      final BooleanSupplier metricsEnabled) {
    this.partitionId = Integer.toString(partitionId);
    this.initialValue = initialValue;

    nextValueKey = new DbString();
    nextValueColumnFamily =
        zeebeDb.createColumnFamily(columnFamily, transactionContext, nextValueKey, nextValue);
    this.metricsEnabled = metricsEnabled;
  }

  public long increment(final String key) {
    final long currentValue = getCurrentValue(key);
    final long next = currentValue + 1;
    nextValue.set(next);
    nextValueColumnFamily.upsert(nextValueKey, nextValue);

    if (metricsEnabled.getAsBoolean()) {
      STATE_COUNTER.labels(partitionId, key).inc();
    }

    return next;
  }

  public long decrement(final String key) {
    final long currentValue = getCurrentValue(key);
    final long next = currentValue - 1;
    nextValue.set(next);
    nextValueColumnFamily.upsert(nextValueKey, nextValue);

    if (metricsEnabled.getAsBoolean()) {
      STATE_COUNTER.labels(partitionId, key).dec();
    }

    return next;
  }

  @Override
  public void onRecovered(final ReadonlyStreamProcessorContext context) {
    nextValueColumnFamily.forEach(
        (type, value) -> STATE_COUNTER.labels(partitionId, type.toString()).set(value.get()));
  }
  //
  //  private void setValue(final String key, final long value) {
  //    nextValueKey.wrapString(key);
  //    nextValue.set(value);
  //    nextValueColumnFamily.upsert(nextValueKey, nextValue);
  //  }

  private long getCurrentValue(final String key) {
    nextValueKey.wrapString(key);
    return getCurrentValue();
  }

  private long getCurrentValue() {
    final NextValue readValue = nextValueColumnFamily.get(nextValueKey);

    long currentValue = initialValue;
    if (readValue != null) {
      currentValue = readValue.get();
    }
    return currentValue;
  }
}
