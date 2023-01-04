/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.util;

import io.camunda.zeebe.util.buffer.BufferWriter;
import java.nio.ByteOrder;
import org.agrona.sbe.MessageEncoderFlyweight;

public final class SbeUtil {
  private SbeUtil() {}

  /**
   * Writes a SBE message as a data property of another message. Using this avoids allocating a
   * temporary buffer to write out the nested property first.
   *
   * @param parentEncoder the parent message encoder
   * @param nestedWriter the nested buffer writer
   * @param nestedHeaderLength the length of the header of the data field
   * @param endianness the byte order of the parent encoder
   */
  public static void writeNestedMessage(
      final MessageEncoderFlyweight parentEncoder,
      final BufferWriter nestedWriter,
      final int nestedHeaderLength,
      final ByteOrder endianness) {
    final var nestedLength = nestedWriter.getLength();
    final var limit = parentEncoder.limit();
    final var writeBuffer = parentEncoder.buffer();

    // encoding of nested messages is done by writing the length of the message as a prefix, then
    // the serialized message
    writeBuffer.putInt(limit, nestedLength, endianness);
    nestedWriter.write(writeBuffer, limit + nestedHeaderLength);

    // simulate updating the offset/limit after writing
    parentEncoder.limit(limit + nestedHeaderLength + nestedLength);
  }
}
