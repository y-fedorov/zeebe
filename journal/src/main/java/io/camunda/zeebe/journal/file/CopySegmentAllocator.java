/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.file.LinuxFs.Advice;
import io.camunda.zeebe.util.VisibleForTesting;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.agrona.IoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SegmentAllocator} implementation which creates a template file and copies it using
 * `sendfile` instead of writing out a file every time.
 */
public final class CopySegmentAllocator implements SegmentAllocator {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopySegmentAllocator.class);
  private static final String TEMPLATE_FILENAME = ".segment.tpl";

  private final Path templateFile;
  private final LinuxFs linuxFs;

  private CopySegmentAllocator(final Path templateFile, final LinuxFs linuxFs) {
    this.templateFile = Objects.requireNonNull(templateFile, "must specify a template file");
    this.linuxFs = linuxFs;
  }

  @Override
  public void allocate(final FileDescriptor fd, final FileChannel channel, final long segmentSize)
      throws IOException {
    try (final var file = new RandomAccessFile(templateFile.toFile(), "r")) {
      if (linuxFs.isCopyFileRangeEnabled()) {
        try {
          linuxFs.copyFileRange(file.getFD(), fd, segmentSize);
        } catch (final UnsupportedOperationException e) {
          LOGGER.warn(
              "Failed to use copy_file_range optimization, disabling it. Will use built-in transferTo as fallback");
          linuxFs.disableCopyFileRange();
          file.getChannel().transferTo(0, segmentSize, channel);
        }
      } else {
        file.getChannel().transferTo(0, segmentSize, channel);
      }
    }
  }

  @Override
  public void onMemoryMapped(final MappedByteBuffer buffer) {
    if (!linuxFs.isMadviseEnabled()) {
      return;
    }

    try {
      linuxFs.madvise(buffer, buffer.remaining(), Advice.MADV_SEQUENTIAL);
    } catch (final UnsupportedOperationException e) {
      LOGGER.warn(
          "Failed to use native system call to advise filesystem, will use fallback from now on",
          e);
      linuxFs.disableMadvise();
    }
  }

  public static CopySegmentAllocator createAllocator(
      final Path segmentDirectory, final long segmentSize) {
    return createAllocator(segmentDirectory, segmentSize, new LinuxFs());
  }

  @VisibleForTesting
  static CopySegmentAllocator createAllocator(
      final Path segmentDirectory, final long segmentSize, final LinuxFs linuxFs) {
    final var templatePath = segmentDirectory.resolve(TEMPLATE_FILENAME);

    try (final var templateChannel =
        FileChannel.open(templatePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
      final var templateSize = Files.size(templatePath);
      if (templateSize < segmentSize) {
        IoUtil.fill(templateChannel, templateSize, segmentSize, (byte) 0);
      } else if (templateSize > segmentSize) {
        templateChannel.truncate(segmentSize);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }

    return new CopySegmentAllocator(templatePath, linuxFs);
  }
}
