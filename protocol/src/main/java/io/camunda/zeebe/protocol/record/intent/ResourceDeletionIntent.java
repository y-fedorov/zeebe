/*
 * Copyright © 2022 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.zeebe.protocol.record.intent;

/**
 * Given resource exists on partitions 1 and 2 First we receive a DELETE command on partition 1
 * (default distro partition) Then, we need to keep track that deletion has started on partition 1
 * and partition 1 must know that partition 2 has not yet deleted it
 * (DeploymentDistribution.DELETING event) Then, we can send a delete request to partition 2 we sent
 * it by sending a SBE message from partition 1 to partition 2 partition 2's sbe handler then writes
 * a command (ResourceDeletionIntent.DISTRIBUTE) on its logstream Then, partition 1 writes
 * DeploymentDistribution.DISTRIBUTING for partition 2
 *
 * <p>On partition 2 we receive the delete request (ResourceDeletionIntent.DISTRIBUTE) THEN,
 * partition 2 deletes the deployment by writing ResourceDeletionIntent.DISTRIBUTED event (applier
 * results in removing the deployment)
 *
 * <p>After deleting the deployment on partition 2, it acks it by sending a request to partition 1
 * (DeploymentDistribution.COMPLETE command for partition 2)
 *
 * <p>partition 1 (DeploymentDistribution.COMPLETE command) processing results in
 * DeploymentDistribution.COMPLETED (removing the pending state)
 *
 * <p>// do we want to wait with deleting the resource from partition 1 until all have acknowledged
 * deletion, or do we do it as soon as possible. We should do it ASAP, to avoid that an unhealthy
 * partition blocks deletion from others Finally, we can delete the resource from partition 1
 * (ResourceDeletionIntent.DISTRIBUTED)
 *
 * <p>distribution is done with DISTRUBTE, DISTRIBUTING, FULLY_DISTRIBUTED
 */
public enum ResourceDeletionIntent implements Intent {
  DELETE((short) 0), // command
  DELETED((short) 1); // event

  private final short value;

  ResourceDeletionIntent(final short value) {
    this.value = value;
  }

  public short getIntent() {
    return value;
  }

  public static Intent from(final short value) {
    switch (value) {
      case 0:
        return DELETE;
      case 1:
        return DELETED;
      default:
        return UNKNOWN;
    }
  }

  @Override
  public short value() {
    return value;
  }
}
