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
package org.neo4j.gds;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.logging.LogAdapter;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.ProcedureCallContextReturnColumns;
import org.neo4j.gds.procedures.TransactionCloseableResourceRegistry;
import org.neo4j.gds.procedures.TransactionNodeLookup;
import org.neo4j.gds.procedures.integration.TransactionTerminationMonitor;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.gds.transaction.EmptyTransactionContext;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

public abstract class BaseProc {

    public static final String ESTIMATE_DESCRIPTION = "Returns an estimation of the memory consumption for that procedure.";

    @Context
    public GraphDatabaseService databaseService;

    @Context
    public Log log;

    @Context
    public Transaction procedureTransaction;

    @Context
    public KernelTransaction transaction;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public TaskRegistryFactory taskRegistryFactory;

    @Context
    public UserLogRegistryFactory userLogRegistryFactory;

    @Context
    public Username username = Username.EMPTY_USERNAME;

    @Context
    public MetricsFacade metricsFacade;

    @Context
    public GraphDataScienceProcedures graphDataScienceProcedures;

    protected String username() {
        return username.username();
    }

    protected boolean isGdsAdmin() {
        if (transaction == null) {
            // No transaction available (likely we're in a test), no-one is admin here
            return false;
        }
        // this should be the same as the predefined role from enterprise-security
        // com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN
        String PREDEFINED_ADMIN_ROLE = "admin";
        return transaction.securityContext().roles().contains(PREDEFINED_ADMIN_ROLE);
    }

    public ExecutionContext executionContext() {
        return databaseService == null
            ? ExecutionContext.EMPTY
            : ImmutableExecutionContext
                .builder()
                .databaseId(databaseId())
                .dependencyResolver(GraphDatabaseApiProxy.dependencyResolver(databaseService))
                .log(new LogAdapter(log))
                .returnColumns(new ProcedureCallContextReturnColumns(callContext))
                .userLogRegistryFactory(userLogRegistryFactory)
                .taskRegistryFactory(taskRegistryFactory)
                .username(username())
                .terminationMonitor(new TransactionTerminationMonitor(transaction))
                .closeableResourceRegistry(new TransactionCloseableResourceRegistry(transaction))
                .nodeLookup(new TransactionNodeLookup(transaction))
                .isGdsAdmin(transactionContext().isGdsAdmin())
                .metricsFacade(metricsFacade)
                .algorithmsProcedureFacade(graphDataScienceProcedures.algorithms())
                .build();
    }

    protected TransactionContext transactionContext() {
        return databaseService == null
            ? EmptyTransactionContext.INSTANCE
            : DatabaseTransactionContext.of(databaseService, procedureTransaction);
    }

    private DatabaseId databaseId() {
        return DatabaseId.of(databaseService.databaseName());
    }
}
