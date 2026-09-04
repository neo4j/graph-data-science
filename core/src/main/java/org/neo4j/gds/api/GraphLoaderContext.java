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

import org.neo4j.gds.logging.Log;
import org.neo4j.gds.progress.registration.EmptyTaskRegistryFactory;
import org.neo4j.gds.progress.registration.TaskRegistryFactory;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.transaction.TransactionContext;

public record GraphLoaderContext(
    TransactionContext transactionContext,
    DatabaseId databaseId,
    Log log,
    TerminationFlag terminationFlag,
    TaskRegistryFactory taskRegistryFactory
) {

    public GraphLoaderContext(
        TransactionContext transactionContext,
        DatabaseId databaseId,
        Log log,
        TerminationFlag terminationFlag
    ) {
        this(
            transactionContext,
            databaseId,
            log,
            terminationFlag,
            TaskRegistryFactory.empty()
        );
    }

    public GraphLoaderContext(TransactionContext transactionContext, DatabaseId databaseId) {
        this(
            transactionContext,
            databaseId,
            Log.noOpLog(),
            TerminationFlag.RUNNING_TRUE,
            TaskRegistryFactory.empty()
        );
    }

    public GraphLoaderContext(TransactionContext transactionContext) {
        this(
            transactionContext,
            DatabaseId.EMPTY,
            Log.noOpLog(),
            TerminationFlag.RUNNING_TRUE,
            TaskRegistryFactory.empty()
        );
    }

    public static final GraphLoaderContext NULL_CONTEXT = new GraphLoaderContext(
        null,
        null,
        Log.noOpLog(),
        TerminationFlag.RUNNING_TRUE,
        EmptyTaskRegistryFactory.INSTANCE
    );

    public static GraphLoaderContext emptyWithTransactionContext(TransactionContext transactionContext) {
        return new GraphLoaderContext(
            transactionContext,
            DatabaseId.EMPTY,
            Log.noOpLog(),
            TerminationFlag.RUNNING_TRUE,
            TaskRegistryFactory.empty()
        );
    }
}
