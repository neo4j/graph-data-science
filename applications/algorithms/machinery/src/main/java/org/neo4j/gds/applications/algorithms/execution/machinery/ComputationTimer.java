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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import java.util.function.LongSupplier;

/**
 * This captures timing for a computation: start time and duration
 */
class ComputationTimer {
    private final LongSupplier timeSupplier;

    private long startTimeInEpochMilliseconds = -1;
    private long endTimeInEpochMilliseconds = -1;

    ComputationTimer(LongSupplier timeSupplier) {
        this.timeSupplier = timeSupplier;
    }

    static ComputationTimer create() {
        return new ComputationTimer(System::currentTimeMillis);
    }

    Session start() {
        startTimeInEpochMilliseconds = timeSupplier.getAsLong();
        return new Session();
    }

    long startTimeInEpochMilliseconds() {
        return startTimeInEpochMilliseconds;
    }

    long durationInMilliseconds() {
        return endTimeInEpochMilliseconds - startTimeInEpochMilliseconds;
    }

    class Session implements AutoCloseable {
        @Override
        public void close() {
            endTimeInEpochMilliseconds = timeSupplier.getAsLong();
        }
    }
}
