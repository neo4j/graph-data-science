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
package org.neo4j.graphalgo.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
    public static <T extends Throwable> T chain(T initial, T current) {
        if (initial == null) {
            return current;
        }

        if (current != null) {
            initial.addSuppressed(current);
        }
        return initial;
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

    public static <T, R, E extends Exception> Function<? super T, ? extends R> function(
        CheckedFunction<? super T, ? extends R, E> function
    ) throws E {
        return function;
    }

    public static <T, E extends Exception> Supplier<? extends T> supplier(CheckedSupplier<? extends T, E> supplier) {
        return supplier;
    }

    public static <T, R, E extends Exception> R apply(
        CheckedFunction<? super T, ? extends R, E> function,
        T input
    ) {
        return function.apply(input);
    }

    public static void validateTargetNodeIsLoaded(long mappedId, long neoId) {
        validateNodeIsLoaded(mappedId, neoId, "target");
    }

    public static void validateSourceNodeIsLoaded(long mappedId, long neoId) {
        validateNodeIsLoaded(mappedId, neoId, "source");
    }

    private static void validateNodeIsLoaded(long mappedId, long neoId, String side) {
        if (mappedId == -1) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Failed to load a relationship because its %s-node with id %s is not part of the node query or projection. " +
                    "To ignore the relationship, set the configuration parameter `validateRelationships` to false.",
                    side,
                    neoId
                )
            );
        }
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
