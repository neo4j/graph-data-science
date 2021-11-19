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

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.transaction.SecurityContextWrapper;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Collection;
import java.util.function.Supplier;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class BaseProc {

    protected static final String ESTIMATE_DESCRIPTION = "Returns an estimation of the memory consumption for that procedure.";

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public Transaction procedureTransaction;

    @Context
    public KernelTransaction transaction;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public AllocationTracker allocationTracker;

    @Context
    public TaskRegistryFactory taskRegistryFactory;

    @Context
    public Username username = Username.EMPTY_USERNAME;

    protected BaseProc() {
        if (allocationTracker == null) {
            allocationTracker = AllocationTracker.empty();
        }
    }

    protected AllocationTracker allocationTracker() {
        return allocationTracker;
    }

    protected String username() {
        return username.username();
    }

    protected NamedDatabaseId databaseId() {
        return api.databaseId();
    }

    protected GraphStoreWithConfig graphStoreFromCatalog(String graphName, BaseConfig config) {
        return GraphStoreFromCatalogLoader.graphStoreFromCatalog(graphName, config, username(), databaseId(), isGdsAdmin());
    }

    protected boolean isGdsAdmin() {
        if (transaction == null) {
            // No transaction available (likely we're in a test), no-one is admin here
            return false;
        }
        return GraphDatabaseApiProxy
            .resolveDependency(api, SecurityContextWrapper.class)
            .isAdmin(transaction.securityContext());
    }

    protected final void runWithExceptionLogging(String message, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.warn(message, e);
            throw e;
        }
    }

    protected final <R> R runWithExceptionLogging(String message, Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            log.warn(message, e);
            throw e;
        }
    }

    protected final void validateConfig(CypherMapWrapper cypherConfig, BaseConfig config) {
        validateConfig(cypherConfig, config.configKeys());
    }

    protected final void validateConfig(CypherMapWrapper cypherConfig, Collection<String> allowedKeys) {
        cypherConfig.requireOnlyKeysFrom(allowedKeys);
    }

    protected final void validateGraphName(String username, String graphName) {
        CypherMapWrapper.failOnBlank("graphName", graphName);
        if (GraphStoreCatalog.exists(username, databaseId(), graphName)) {
            throw new IllegalArgumentException(formatWithLocale(
                "A graph with name '%s' already exists.",
                graphName
            ));
        }
    }

    protected GraphLoaderContext graphLoaderContext() {
        return ImmutableGraphLoaderContext.builder()
            .transactionContext(TransactionContext.of(api, procedureTransaction))
            .api(api)
            .log(log)
            .allocationTracker(allocationTracker)
            .taskRegistryFactory(taskRegistryFactory)
            .terminationFlag(TerminationFlag.wrap(transaction))
            .build();
    }

    public MemoryUsageValidator memoryUsageValidator() {
        return new MemoryUsageValidator(log, api);
    }

    @FunctionalInterface
    public interface FreeMemoryInspector {
        long freeMemory();
    }
}
