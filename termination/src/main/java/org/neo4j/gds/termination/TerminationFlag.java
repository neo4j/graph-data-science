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
package org.neo4j.gds.termination;

import java.util.Optional;
import java.util.function.Supplier;

@FunctionalInterface
public interface TerminationFlag {

    TerminationFlag RUNNING_TRUE = () -> true;
    TerminationFlag DEFAULT = () -> true;
    TerminationFlag STOP_RUNNING = () -> false;

    int RUN_CHECK_NODE_COUNT = 10_000;

    /**
     * Creates a new termination flag.
     *
     * @param terminationMonitor used to signal that the execution stopped running
     */
    static TerminationFlag wrap(TerminationMonitor terminationMonitor) {
        return new TerminationFlagImpl(terminationMonitor, Optional.empty());
    }

    /**
     * Creates a new termination flag.
     *
     * @param terminationMonitor used to signal that the execution stopped running
     * @param terminationCause returns a {@link RuntimeException} that is thrown when the execution is terminated
     */
    static TerminationFlag wrap(TerminationMonitor terminationMonitor, Supplier<RuntimeException> terminationCause) {
        return new TerminationFlagImpl(terminationMonitor, Optional.of(terminationCause));
    }

    boolean running();

    /**
     * @throws RuntimeException if the transaction has been terminated
     */
    default void assertRunning() {
        if (!running()) {
            terminate();
        }
    }

    default void terminate() {
        throw new TerminatedException();
    }
}
