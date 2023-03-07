/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.utils;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ExceptionUtil {

    /**
     * Returns the root cause of an exception.
     *
     * Copied from {@code org.neo4j.helpers.Exceptions#rootCause(Throwable)} due to deprecation.
     *
     * @param caughtException exception to find the root cause of.
     * @return the root cause.
     * @throws IllegalArgumentException if the provided exception is null.
     */
    public static Throwable rootCause(Throwable caughtException) {
        if (null == caughtException) {
            throw new IllegalArgumentException("Cannot obtain rootCause from (null)");
        }
        Throwable root = caughtException;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }

    /**
     * Adds the current exception to the initial exception as suppressed.
     *
     * Copied from {@code org.neo4j.helpers.Exceptions#chain(Throwable, Throwable)} due to deprecation.
     */
    @Contract("null, _ -> param2; !null, _ -> !null")
    public static <T extends Throwable> @Nullable T chain(@Nullable T initial, @Nullable T current) {
        if (initial == null) {
            return current;
        }

        if (current != null) {
            initial.addSuppressed(current);
        }
        return initial;
    }

    public static final CheckedConsumer<Exception, Exception> RETHROW_CHECKED = e -> {
        throw e;
    };

    public static final CheckedConsumer<Exception, RuntimeException> RETHROW_UNCHECKED = e -> {
        ExceptionUtil.throwIfUnchecked(e);
        throw new RuntimeException(e);
    };

    public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
        closeAll(closeables.iterator());
    }

    public static void closeAll(Iterator<? extends AutoCloseable> closeables) throws Exception {
        closeAll(RETHROW_CHECKED, closeables);
    }

    public static <E extends Exception> void closeAll(
        CheckedConsumer<Exception, E> handler,
        AutoCloseable... closeables
    ) throws E {
        closeAll(handler, Arrays.asList(closeables));
    }

    public static <E extends Exception> void closeAll(
        CheckedConsumer<Exception, E> handler,
        Iterable<? extends AutoCloseable> closeables
    ) throws E {
        closeAll(handler, closeables.iterator());
    }

    public static <E extends Exception> void closeAll(
        CheckedConsumer<Exception, E> handler,
        Iterator<? extends AutoCloseable> closeables
    ) throws E {
        Exception error = null;
        while (closeables.hasNext()) {
            var closeable = closeables.next();
            try {
                closeable.close();
            } catch (Exception e) {
                error = chain(error, e);
            }
        }
        if (error != null) {
            handler.checkedAccept(error);
        }
    }

    /**
     * Rethrows {@code exception} if it is an instance of {@link RuntimeException} or {@link Error}. Typical usage is:
     *
     * <pre>
     * catch (Throwable e) {
     *   ......common code......
     *   throwIfUnchecked(e);
     *   throw new RuntimeException(e);
     * }
     * </pre>
     *
     * This will only wrap checked exception in a {@code RuntimeException}. Do note that if the segment {@code common code}
     * is missing, it's preferable to use this instead:
     *
     * <pre>
     * catch (RuntimeException | Error e) {
     *   throw e;
     * }
     * catch (Throwable e) {
     *   throw new RuntimeException(e);
     * }
     * </pre>
     *
     * @param exception to rethrow.
     */
    public static void throwIfUnchecked(Throwable exception) {
        Objects.requireNonNull(exception);
        if (exception instanceof RuntimeException) {
            throw (RuntimeException) exception;
        }
        if (exception instanceof IOException) {
            throw new UncheckedIOException((IOException) exception);
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
    }

    @SuppressWarnings("TypeMayBeWeakened")
    public static <E extends Exception> Runnable unchecked(CheckedRunnable<E> runnable) {
        return runnable;
    }

    @SuppressWarnings("TypeMayBeWeakened")
    public static <E extends Exception> void run(CheckedRunnable<E> runnable) {
        runnable.run();
    }

    public static <T, E extends Exception> Consumer<T> consumer(CheckedConsumer<T, E> consumer) {
        return consumer;
    }

    public static <T, R, E extends Exception> Function<T, R> function(CheckedFunction<T, R, E> function) {
        return function;
    }

    public static <T, E extends Exception> Supplier<? extends T> supplier(CheckedSupplier<? extends T, E> supplier) {
        return supplier;
    }

    public static <T, E extends Exception> T supply(CheckedSupplier<? extends T, E> supplier) {
        return supplier.get();
    }

    public static <T, R, E extends Exception> R apply(
        CheckedFunction<? super T, ? extends R, E> function,
        T input
    ) {
        return function.apply(input);
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
