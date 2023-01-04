/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.journal.file;

import io.camunda.zeebe.util.Loggers;
import java.util.Map;
import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.ffi.types.ssize_t;

/**
 * Used to bind certain calls from libc to Java methods via JNA.
 *
 * <p>Note that method names do not follow our conventions, but this is necessary here because the
 * names must match those of the C library.
 *
 * <p>See {@link #ofNativeLibrary()} for an example of how to use this.
 *
 * <p>NOTE: this interface must be public for JNR-FFI to generate the bindings. However, this can
 * most likely be replaced in the future once Project Panama is part of the JDK, and, using
 * jextract, we can get compile time bindings.
 */
@SuppressWarnings({"checkstyle:methodname", "unused"})
public interface LibC {
  // https://man7.org/linux/man-pages/man3/posix_fallocate.3.html
  int posix_fallocate(final @In int fd, final @In @off_t long offset, final @In @size_t long len);

  // https://man7.org/linux/man-pages/man2/fallocate.2.html
  int fallocate(
      final @In int fd,
      final @In int mode,
      final @In @off_t long offset,
      final @In @size_t long len);

  // https://man7.org/linux/man-pages/man2/madvise.2.html
  int posix_madvise(final @In Pointer addr, final @In @size_t long length, final int advice);

  @SuppressWarnings("checkstyle:ParameterName")
  int sendfile(
      final @In int out_fd,
      final @In int in_fd,
      final @Out Pointer offset,
      final @In @size_t long count);

  @SuppressWarnings("checkstyle:ParameterName")
  @ssize_t
  long copy_file_range(
      final @In int fd_in,
      final @Out Pointer off_in,
      final @In int fd_out,
      final @Out Pointer off_out,
      final @In @size_t long len,
      final int flags);

  /**
   * Returns an instance of LibC bound to the system's C library (e.g. glibc, musl, etc.).
   *
   * <p>If it fails to bind to the C library, it will return a {@link InvalidLibC} instance which
   * throws {@link UnsupportedOperationException} on every call.
   *
   * @return an instance of this library
   */
  static LibC ofNativeLibrary() {
    try {
      return LibraryLoader.loadLibrary(
          LibC.class,
          Map.of(LibraryOption.LoadNow, true),
          Platform.getNativePlatform().getStandardCLibraryName());
    } catch (final UnsatisfiedLinkError e) {
      Loggers.FILE_LOGGER.warn(
          "Failed to load C library; any native calls will not be available", e);
      return new InvalidLibC();
    }
  }

  /**
   * Dummy implementation which throws {@link UnsupportedOperationException} on every call.
   * Explicitly left non-final so test classes can extend it and overload only these methods they
   * care about.
   */
  class InvalidLibC implements LibC {

    @Override
    public int posix_fallocate(final int fd, final long offset, final long len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int fallocate(final int fd, final int mode, final long offset, final long len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int posix_madvise(final Pointer addr, final long length, final int advice) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("checkstyle:ParameterName")
    @Override
    public int sendfile(final int out_fd, final int in_fd, final Pointer offset, final long count) {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("checkstyle:ParameterName")
    @Override
    public long copy_file_range(
        final int fd_in,
        final Pointer off_in,
        final int fd_out,
        final Pointer off_out,
        final long len,
        final int flags) {
      throw new UnsupportedOperationException();
    }
  }
}
