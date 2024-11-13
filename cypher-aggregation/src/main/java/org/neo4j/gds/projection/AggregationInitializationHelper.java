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
package org.neo4j.gds.projection;

import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.impl.api.KernelTransactions;

public final class AggregationInitializationHelper {

    private AggregationInitializationHelper() {}

    public static boolean runsOnCompositeDatabase(Context ctx) {
        var databaseService = ctx.graphDatabaseAPI();
        var databaseId = GraphDatabaseApiProxy.databaseId(databaseService);
        var repo = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseReferenceRepository.class);
        return repo.getCompositeDatabaseReferences()
            .stream()
            .map(DatabaseReferenceImpl.Internal::databaseId)
            .anyMatch(databaseId::equals);
    }

    public static ExecutingQueryProvider getQueryProvider(Context ctx, boolean runsOnCompositeDatabase) throws ProcedureException {
        var databaseService = ctx.graphDatabaseAPI();
        var transaction = ctx.transaction();
        ExecutingQueryProvider queryProvider;
        if (runsOnCompositeDatabase) {
            queryProvider = ExecutingQueryProvider.empty();
        } else {
            assert GraphDatabaseApiProxy.containsDependency(databaseService, KernelTransactions.class);
            var ktxs = GraphDatabaseApiProxy.resolveDependency(databaseService, KernelTransactions.class);
            queryProvider = ExecutingQueryProvider.fromTransaction(ktxs, transaction);
        }

        return queryProvider;
    }
}
