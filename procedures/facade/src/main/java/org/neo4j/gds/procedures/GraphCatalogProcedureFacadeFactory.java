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
package org.neo4j.gds.procedures;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.GraphProjectMemoryUsageService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.procedures.catalog.DatabaseModeRestriction;
import org.neo4j.gds.procedures.catalog.GraphCatalogProcedureFacade;
import org.neo4j.gds.procedures.catalog.LocalGraphCatalogProcedureFacade;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.function.Consumer;

/**
 * Here we keep everything related to constructing the {@link org.neo4j.gds.procedures.catalog.LocalGraphCatalogProcedureFacade}
 * from a {@link org.neo4j.kernel.api.procedure.Context}, at request time.
 * <p>
 * We can resolve things like user and database id here, construct termination flags, and such.
 */
public class GraphCatalogProcedureFacadeFactory {
    // dull bits
    private final TransactionContextAccessor transactionContextAccessor = new TransactionContextAccessor();

    // Global scoped/ global state/ stateless things
    private final Log log;

    /**
     * We inject services here so that we may control and isolate access to dependencies.
     * Take {@link org.neo4j.gds.procedures.UserAccessor} for example.
     * Without it, I would have to stub out Neo4j's {@link org.neo4j.kernel.api.procedure.Context}, in a non-trivial,
     * ugly way. Now instead I can inject the user by stubbing out GDS' own little POJO service.
     */
    public GraphCatalogProcedureFacadeFactory(Log log) {
        this.log = log;
    }

    /**
     * We construct the catalog facade at request time. At this point things like user and database id are set in stone.
     * And we can readily construct things like termination flags.
     */
    GraphCatalogProcedureFacade createGraphCatalogProcedureFacade(
        ApplicationsFacade applicationsFacade,
        GraphDatabaseService graphDatabaseService,
        KernelTransaction kernelTransaction,
        Transaction procedureTransaction,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        ProcedureReturnColumns procedureReturnColumns,
        MemoryTracker memoryTracker
    ) {
        // Derived data and services
        var graphProjectMemoryUsageService = new GraphProjectMemoryUsageService(
            requestScopedDependencies.user().getUsername(),
            log,
            graphDatabaseService,
            memoryTracker
        );

        var streamCloser = new Consumer<AutoCloseable>() {
            @Override
            public void accept(AutoCloseable autoCloseable) {
                kernelTransaction.resourceMonitor().registerCloseableResource(autoCloseable);
            }
        };
        var transactionContext = transactionContextAccessor.transactionContext(
            graphDatabaseService,
            procedureTransaction
        );

        var databaseModeRestriction = new DatabaseModeRestriction(graphDatabaseService);

        return new LocalGraphCatalogProcedureFacade(
            requestScopedDependencies,
            streamCloser,
            graphDatabaseService,
            graphProjectMemoryUsageService,
            transactionContext,
            applicationsFacade.graphCatalog(),
            writeContext,
            procedureReturnColumns,
            databaseModeRestriction
        );
    }
}
