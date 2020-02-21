/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.compat;

import java.util.Objects;

public final class ExceptionUtil {

    /**
     * Returns the root cause of an exception.
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
        if (exception instanceof Error) {
            throw (Error) exception;
        }
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
