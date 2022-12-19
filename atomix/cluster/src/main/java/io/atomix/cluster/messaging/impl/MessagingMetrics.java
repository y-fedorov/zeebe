/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.atomix.cluster.messaging.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

final class MessagingMetrics {

  //  private static final Histogram REQUEST_RESPONSE_LATENCY =
  //      Histogram.build()
  ////          .exponentialBuckets(0.0001, 4, 10)
  //          .namespace("zeebe")
  //          .name("messaging_request_response_latency")
  //          .help("The time how long it takes to respond to a request")
  //          .labelNames("topic")
  //          .register();

  private static final Counter CONNECTION_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("messaging_connection_count")
          .help("Number of new connections created")
          .labelNames("address")
          .register();

  private static final Gauge OPEN_CONNECTIONS =
      Gauge.build()
          .namespace("zeebe")
          .name("messaging_open_connections")
          .help("The open connections")
          .register();

  private static final Counter REQUEST_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("messaging_request_count")
          .help("Number of requests has been send to a certain address")
          .labelNames("address", "topic")
          .register();

  private static final Counter RESPONSE_COUNT =
      Counter.build()
          .namespace("zeebe")
          .name("messaging_response_count")
          .help("Number of responses which has been received")
          .labelNames("address", "topic", "outcome")
          .register();

  private static final Gauge IN_FLIGHT_REQUESTS =
      Gauge.build()
          .namespace("zeebe")
          .name("messaging_inflight_requests")
          .help("The inflight requests, open futures")
          .labelNames("address", "topic")
          .register();

  //  Histogram.Timer startRequestTimer(final String name) {
  //    return REQUEST_RESPONSE_LATENCY.labels(name).startTimer();
  //  }

  void countRequest(final String to, final String name) {
    REQUEST_COUNT.labels(to, name).inc();
  }

  void countConnection(final String to) {
    CONNECTION_COUNT.labels(to).inc();
  }

  void countSuccessResponse(final String address, final String name) {
    RESPONSE_COUNT.labels(address, name, "SUCCESS").inc();
  }

  void countFailureResponse(final String address, final String name, String error) {
    RESPONSE_COUNT.labels(address, name, error).inc();
  }

  void updateInFlightRequests(final String address, String topic, final int length) {
    IN_FLIGHT_REQUESTS.labels(address, topic).set(length);
  }

  void updateOpenConnections(final int count) {
    OPEN_CONNECTIONS.labels().set(count);
  }
}
