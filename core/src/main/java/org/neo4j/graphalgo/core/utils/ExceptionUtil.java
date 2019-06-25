/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;

public final class ExceptionUtil {

    /**
     * Returns the root cause of an exception.
     *
     * Copied from {@link org.neo4j.helpers.Exceptions#rootCause(Throwable)} due to deprecation.
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

    public static RuntimeException asUnchecked(final Throwable exception) {
        if (exception instanceof RuntimeException) {
            return (RuntimeException) exception;
        }
        if (exception instanceof Error) {
            throw (Error) exception;
        }
        return new RuntimeException(exception);
    }

    public static <T> T throwKernelException(KernelException e) {
        Status status = e.status();
        String codeString = status.code().serialize();
        String message = e.getMessage();
        String newMessage;
        if (message == null || message.isEmpty()) {
            newMessage = codeString;
        } else {
            newMessage = codeString + ": " + message;
        }
        throw new RuntimeException(newMessage, e);
    }

    private ExceptionUtil() {
        throw new UnsupportedOperationException("No instances");
    }
}
