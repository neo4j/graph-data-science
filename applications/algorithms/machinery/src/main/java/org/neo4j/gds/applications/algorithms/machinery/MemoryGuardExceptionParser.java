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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.memory.tracking.MemoryGuardException;
import org.neo4j.gds.memory.tracking.MemoryReservationExceededException;
import org.neo4j.gds.memory.tracking.MemoryReservationExceededTotalMemoryException;
import org.neo4j.gds.utils.StringFormatting;

public final class MemoryGuardExceptionParser {

    private MemoryGuardExceptionParser() {}

    static void transformException(Label label, MemoryGuardException e) {
        String message;
        if (e instanceof MemoryReservationExceededTotalMemoryException exceededTotalException) {
            message = StringFormatting.formatWithLocale(
                "Memory required to run %s (%db) exceeds total available memory (%db)",
                label.asString(),
                exceededTotalException.bytesRequired(),
                exceededTotalException.bytesAvailable()
            );
            throw new IllegalStateException(message);
        } else if (e instanceof MemoryReservationExceededException exceededException) {
            message = StringFormatting.formatWithLocale(
                "Memory required to run %s (%db) exceeds current available memory (%db)",
                label.asString(),
                exceededException.bytesRequired(),
                exceededException.bytesAvailable()
            );
            throw new IllegalStateException(message);

        } else {
            throw new RuntimeException("Unrecognized exception: " + e.getClass().getSimpleName());
        }
    }
}
