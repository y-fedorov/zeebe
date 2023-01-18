/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.journal.JournalException.OutOfDiskSpace;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntSupplier;
import jnr.constants.platform.Errno;
import jnr.ffi.LastError;
import jnr.ffi.Platform;
import jnr.ffi.Platform.OS;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LinuxFs {
  private static final Logger LOGGER = LoggerFactory.getLogger(PosixFs.class);
  private static final VarHandle FILE_DESCRIPTOR_FD_FIELD;

  static {
    VarHandle fileDescriptorFd;
    try {
      fileDescriptorFd =
          MethodHandles.privateLookupIn(FileDescriptor.class, MethodHandles.lookup())
              .findVarHandle(FileDescriptor.class, "fd", int.class);
    } catch (final NoSuchFieldException | IllegalAccessException e) {
      LOGGER.warn("Cannot look up file descriptor via reflection; NativeFS will be disabled", e);
      fileDescriptorFd = null;
    }

    FILE_DESCRIPTOR_FD_FIELD = fileDescriptorFd;
  }

  // by default, we assume only Linux supports fallocate. some may not, and some file systems may
  // not, and normally C libraries will emulate the behavior, but some (e.g. musl) may return
  // EOPNOTSUPP, in which case we want to set this flag to false.
  //
  // note that this flag assumes there is only one underlying filesystem for the whole application
  private volatile boolean supportsFallocate =
      FILE_DESCRIPTOR_FD_FIELD != null && Platform.getNativePlatform().getOS() == OS.LINUX;

  // by default, we assume only Linux platforms support madvise
  private volatile boolean supportsMadvise = Platform.getNativePlatform().getOS() == OS.LINUX;

  // by default, we assume only Linux platforms support copy_file_range
  private volatile boolean supportsCopyFileRange =
      FILE_DESCRIPTOR_FD_FIELD != null
          && Platform.getNativePlatform().getOS() == OS.LINUX
          && Platform.getNativePlatform().getVersionMajor() >= 5
          && Platform.getNativePlatform().getVersionMinor() >= 13;

  private final LibC libC;
  private final IntSupplier errnoSupplier;

  LinuxFs() {
    this(LibC.ofNativeLibrary(), () -> LastError.getLastError(Runtime.getSystemRuntime()));
  }

  LinuxFs(final LibC libC) {
    this(libC, () -> LastError.getLastError(Runtime.getSystemRuntime()));
  }

  LinuxFs(final LibC libC, final IntSupplier errnoSupplier) {
    this.libC = Objects.requireNonNull(libC, "must specify a LibC implementation");
    this.errnoSupplier =
        Objects.requireNonNull(errnoSupplier, "must specify an error code supplier");
  }

  /**
   * Returns whether calls to {@link #fallocate(FileDescriptor, long, long, FallocateFlag...)} are
   * supported or not. If this returns false, then a call to {@link #fallocate(FileDescriptor, long,
   * long, FallocateFlag...)} will throw an {@link UnsupportedOperationException}.
   *
   * @return true if supported, false otherwise
   */
  boolean isFallocateEnabled() {
    return supportsFallocate;
  }

  /**
   * Disables usage of {@link #fallocate(FileDescriptor, long, long, FallocateFlag...)}. After
   * calling this, {@link #isFallocateEnabled()} will return false.
   */
  void disableFallocate() {
    LOGGER.debug("Disabling usage of fallocate optimization");
    supportsFallocate = false;
  }

  /**
   * Calls posix_fallocate system call, delegating the allocation of the blocks to the C library.
   *
   * <p>For glibc, this means it will delegate the call to the file system. If it supports it (e.g.
   * ext4), this is much more performant than writing a file, as the blocks are reserved for the
   * file, but no I/O operations take place (other than updating the file's metadata). Not only
   * this, but the blocks reserved are in most cases contiguous, making for less disk fragmentation.
   *
   * <p>When the file system does not support the call, then the library will emulate it by actually
   * zero-ing the file. In some cases (e.g. musl), the library will do no emulation, and instead we
   * will throw an {@link UnsupportedOperationException} and set {@link #isFallocateEnabled()} to
   * false.
   *
   * <p><a href="https://man7.org/linux/man-pages/man3/posix_fallocate.3.html">See the man pages for
   * posix_fallocate</a>
   *
   * @param descriptor the file descriptor of the file we want to grow
   * @param offset the offset at which we want to start growing the file
   * @param length the length, in bytes, of the region to grow
   * @throws IllegalArgumentException if offset or length is negative
   * @throws InterruptedIOException if an interrupt occurs while it was allocating the file, meaning
   *     it may not have been fully allocated
   * @throws UnsupportedOperationException if the underlying file system does not support this or if
   *     this function is disabled via {@link #disableFallocate()}
   * @throws OutOfDiskSpace if there is not enough disk space to allocate the file
   * @throws IOException if the file descriptor is invalid (e.g. not opened for writing, not
   *     pointing to a regular file, etc.)
   */
  void fallocate(
      final FileDescriptor descriptor,
      final long offset,
      final long length,
      final FallocateFlag... flags)
      throws IOException {
    if (offset < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot allocate file with a negative offset of [%d]", offset));
    }

    if (length < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot allocate file with a negative length of [%d]", length));
    }

    if (!isFallocateEnabled()) {
      throw new UnsupportedOperationException(
          "Failed to pre-allocate file natively: fallocate is disabled");
    }

    final int fd = (int) FILE_DESCRIPTOR_FD_FIELD.get(descriptor);
    final int mode = computeModeFromFlags(flags);
    final int result = libC.fallocate(fd, mode, offset, length);

    // success
    if (result == 0) {
      return;
    }

    final Errno error = Errno.valueOf(errnoSupplier.getAsInt());
    throwExceptionFromErrno(offset, length, error);
  }

  /**
   * Returns whether calls to {@link #copyFileRange(FileDescriptor, FileDescriptor, long)} are
   * supported or not. If this returns false, then a call to {@link #copyFileRange(FileDescriptor,
   * FileDescriptor, long)} will throw an {@link UnsupportedOperationException}.
   *
   * @return true if supported, false otherwise
   */
  boolean isCopyFileRangeEnabled() {
    return supportsCopyFileRange;
  }

  /**
   * Disables usage of {@link #copyFileRange(FileDescriptor, FileDescriptor, long)}. After calling
   * this, {@link #isCopyFileRangeEnabled()} ()} will return false.
   */
  void disableCopyFileRange() {
    LOGGER.debug("Disabling usage of copy_file_range optimization");
    supportsCopyFileRange = false;
  }

  public long copyFileRange(
      final FileDescriptor inDescriptor, final FileDescriptor outDescriptor, final long size)
      throws IOException {
    if (size <= 0) {
      throw new IllegalArgumentException(
          "Expected to copy a file with a positive size, but size is %d".formatted(size));
    }

    if (!isCopyFileRangeEnabled()) {
      throw new UnsupportedOperationException(
          "copyFileRange is not supported by the operating system");
    }

    final int inFd =
        (int)
            FILE_DESCRIPTOR_FD_FIELD.get(
                Objects.requireNonNull(inDescriptor, "must specify an input file descriptor"));
    final int outFd =
        (int)
            FILE_DESCRIPTOR_FD_FIELD.get(
                Objects.requireNonNull(outDescriptor, "must specify an output file descriptor"));

    final var result = libC.copy_file_range(inFd, null, outFd, null, size, 0);
    if (result < 0) {
      throwExceptionFromErrno(0, size, Errno.valueOf(errnoSupplier.getAsInt()));
    }

    return result;
  }

  /**
   * Returns whether calls to {@link #madvise(MappedByteBuffer, long, Advice)} are supported or not.
   * If this returns false, then a call to {@link #madvise(MappedByteBuffer, long, Advice)} will
   * throw an {@link UnsupportedOperationException}.
   *
   * @return true if supported, false otherwise
   */
  boolean isMadviseEnabled() {
    return supportsMadvise;
  }

  /**
   * Disables usage of {@link #madvise(MappedByteBuffer, long, Advice)}. After calling this, {@link
   * #isMadviseEnabled()} ()} will return false.
   */
  void disableMadvise() {
    LOGGER.debug("Disabling usage of madvise optimization");
    supportsMadvise = false;
  }

  /**
   * Provides advice to the OS about usage of the memory mapped buffer. See <a
   * href="https://man7.org/linux/man-pages/man3/posix_madvise.3.html">posix_madvise(3)</a> for
   * more.
   *
   * @param buffer the buffer to advise on
   * @param length the length of the range for which the advice is valid
   * @param advice the specific advice
   * @throws IllegalArgumentException if the length is negative, or the advice is not valid on that
   *     system
   * @throws UnsupportedOperationException if the function was previously disabled, or does not
   *     exist on this system
   * @throws IndexOutOfBoundsException if any of the computed page range (using the buffer's start
   *     address up to the given length in bytes) does not belong to this process
   */
  void madvise(final MappedByteBuffer buffer, final long length, final Advice advice) {
    if (length < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot advise system about negative range [%d]", length));
    }

    if (!isMadviseEnabled()) {
      throw new UnsupportedOperationException(
          "Failed to pre-allocate file natively: posix_fallocate is disabled");
    }

    final Pointer address = Pointer.wrap(Runtime.getSystemRuntime(), buffer);
    final int result = libC.posix_madvise(address, length, advice.value);

    // success
    if (result == 0) {
      return;
    }

    final Errno error = Errno.valueOf(result);
    switch (error) {
      case EINVAL -> throw new IllegalArgumentException(
          "Computed address [%s] of the given buffer is not a multiple of the system page size, or advice [%s] is invalid"
              .formatted(address, advice));
      case ENOMEM -> throw new IndexOutOfBoundsException(
          "Addresses in the specified range [%d, %d] are partially or completely outside the caller's address space"
              .formatted(address.address(), address.getAddress(length)));
      default -> throw new UnsupportedOperationException(
          "Failed to provide advice for memory mapping: the underlying filesystem does not support this operation");
    }
  }

  private int computeModeFromFlags(final FallocateFlag... flags) {
    return Arrays.stream(flags).map(FallocateFlag::mode).reduce((m1, m2) -> m1 & m2).orElse(0);
  }

  private void throwExceptionFromErrno(final long offset, final long length, final Errno error)
      throws IOException {
    switch (error) {
      case EBADF -> throw new IOException(
          "Failed to pre-allocate file: it doesn't have a valid file descriptor, or it's not "
              + "opened for writing");
      case EFBIG -> throw new IOException(
          "Failed to pre-allocate file: it's length [%d] would exceed the system's maximum file length"
              .formatted(offset + length));
      case EINVAL -> throw new IllegalArgumentException(
          "Failed to pre-allocate file: it's offset [%d] or length [%d] was less than or equal to 0"
              .formatted(offset, length));
      case EINTR -> throw new InterruptedIOException(
          "Failed to pre-allocate file interrupted during call");
      case ENODEV, ESPIPE -> throw new IOException(
          "Failed to pre-allocate file: the descriptor does not point to a regular file");
      case ENOSPC -> throw new OutOfDiskSpace(
          "Failed to pre-allocate file: there is not enough space");
      default -> throw new UnsupportedOperationException(
          "Failed to pre-allocate file: the underlying filesystem does not support this operation");
    }
  }

  enum FallocateFlag {
    FALLOC_FL_KEEP_SIZE(0x01),
    FALLOC_FL_PUNCH_HOLE(0x02),
    FALLOC_FL_NO_HIDE_STALE(0x04),
    FALLOC_FL_COLLAPSE_RANGE(0x08),
    FALLOC_FL_ZERO_RANGE(0x10),
    FALLOC_FL_INSERT_RANGE(0x20),
    FALLOC_FL_UNSHARE_RANGE(0x40);

    private final int mode;

    FallocateFlag(final int mode) {
      this.mode = mode;
    }

    int mode() {
      return mode;
    }
  }

  enum Advice {
    // No further special treatment
    POSIX_MADV_NORMAL(0),
    // Expect random page references
    MADV_RANDOM(1),
    // Expect sequential page references
    MADV_SEQUENTIAL(2),
    // Will need these pages
    MADV_WILLNEED(3),
    // Don't need these pages
    MADV_DONTNEED(4);

    private final int value;

    Advice(final int value) {
      this.value = value;
    }
  }
}
