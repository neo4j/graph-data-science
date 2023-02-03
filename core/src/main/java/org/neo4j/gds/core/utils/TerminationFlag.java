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
package org.neo4j.gds.core.utils;

import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;

public interface TerminationFlag {

    TerminationFlag RUNNING_TRUE = () -> true;

    int RUN_CHECK_NODE_COUNT = 10_000;

    static TerminationFlag wrap(TerminationMonitor terminationMonitor) {
        return new TerminationFlagImpl(terminationMonitor);
    }

    static TerminationFlag wrap(TerminationMonitor terminationMonitor, long interval) {
        return new TerminationFlagImpl(terminationMonitor).withCheckInterval(interval);
    }

    boolean running();

    /**
     * @throws RuntimeException if the transaction has been terminated
     */
    default void assertRunning() {
        if (!running()) {
            throw new TransactionTerminatedException(Status.Transaction.Terminated);
        }
    }
}
