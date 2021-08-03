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
package org.neo4j.gds.api;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.concurrent.ExecutorService;

@ValueClass
public interface GraphLoaderContext {

    TransactionContext transactionContext();

    GraphDatabaseAPI api();

    Log log();

    @Value.Default
    default ExecutorService executor() {
        return Pools.DEFAULT;
    }

    @Value.Default
    default AllocationTracker tracker() {
        return AllocationTracker.empty();
    }

    @Value.Default
    default TerminationFlag terminationFlag() {
        return TerminationFlag.RUNNING_TRUE;
    }

    GraphLoaderContext NULL_CONTEXT = new GraphLoaderContext() {
        @Override
        public TransactionContext transactionContext() {
            return null;
        }

        @Override
        public GraphDatabaseAPI api() {
            return null;
        }

        @Override
        public Log log() {
            return NullLog.getInstance();
        }
    };
}
