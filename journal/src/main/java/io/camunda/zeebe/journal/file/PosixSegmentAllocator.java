/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.file.PosixFs.Advice;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PosixSegmentAllocator implements SegmentAllocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PosixSegmentAllocator.class);
  private final PosixFs posixFs;
  private final SegmentAllocator fallback;
  private final boolean prefaultMapping;

  PosixSegmentAllocator() {
    this(new PosixFs());
  }

  PosixSegmentAllocator(final PosixFs posixFs) {
    this(posixFs, SegmentAllocator.fill());
  }

  PosixSegmentAllocator(final PosixFs posixFs, final SegmentAllocator fallback) {
    this(posixFs, fallback, true);
  }

  PosixSegmentAllocator(
      final PosixFs posixFs, final SegmentAllocator fallback, final boolean prefaultMapping) {
    this.posixFs = Objects.requireNonNull(posixFs, "must specify a POSIX file system abstraction");
    this.fallback =
        Objects.requireNonNull(
            fallback, "must specify a fallback allocator or SegmentAllocator::noop");
    this.prefaultMapping = prefaultMapping;
  }

  @Override
  public void allocate(
      final FileDescriptor descriptor, final FileChannel segmentChannel, final long segmentSize)
      throws IOException {
    if (!posixFs.isPosixFallocateEnabled()) {
      fallback.allocate(descriptor, segmentChannel, segmentSize);
      return;
    }

    // fallocate and zero-out to prevent penalties on first access
    try {
      posixFs.posixFallocate(descriptor, 0, segmentSize);
      IoUtil.fill(segmentChannel, 0, segmentSize, (byte) 0);
    } catch (final UnsupportedOperationException e) {
      LOGGER.warn(
          "Failed to use native system call to pre-allocate file, will use fallback from now on",
          e);
      posixFs.disablePosixFallocate();
      fallback.allocate(descriptor, segmentChannel, segmentSize);
    }
  }

  @Override
  public void onMemoryMapped(final MappedByteBuffer buffer) {
    if (prefaultMapping) {
      prefaultMapping(buffer);
    }

    madvise(buffer);
  }

  private void prefaultMapping(final MappedByteBuffer buffer) {
    buffer.mark();

    // touch each page with a marker; this will pre-fault the file and also remove the penalty for
    // the first block access
    final var marker = (byte) 0;
    try {
      while (buffer.hasRemaining()) {
        buffer.put(buffer.position(), marker);
        buffer.position(buffer.position() + IoUtil.BLOCK_SIZE);
      }
    } finally {
      buffer.reset();
    }
  }

  private void madvise(final MappedByteBuffer buffer) {
    if (!posixFs.isPosixMadviseEnabled()) {
      return;
    }

    try {
      posixFs.madvise(buffer, buffer.remaining(), Advice.POSIX_MADV_SEQUENTIAL);
    } catch (final UnsupportedOperationException e) {
      LOGGER.warn(
          "Failed to use native system call to advise filesystem, will use fallback from now on",
          e);
      posixFs.disablePosixMadvise();
    }
  }
}
