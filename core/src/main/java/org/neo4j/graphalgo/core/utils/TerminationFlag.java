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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.kernel.api.KernelTransaction;

import static org.neo4j.graphalgo.compat.Transactions.transactionTerminated;

public interface TerminationFlag {

    TerminationFlag RUNNING_TRUE = () -> true;

    int RUN_CHECK_NODE_COUNT = 10_000;

    static TerminationFlag wrap(KernelTransaction transaction) {
        return new TerminationFlagImpl(transaction);
    }

    static TerminationFlag wrap(KernelTransaction transaction, long interval) {
        return new TerminationFlagImpl(transaction).withCheckInterval(interval);
    }

    boolean running();

    /**
     * @throws RuntimeException if the transaction has been terminated
     */
    default void assertRunning() {
        if (!running()) {
            throw transactionTerminated();
        }
    }
}
