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

import org.neo4j.configuration.Config;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.TransactionContext;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.GcListenerExtension;
import org.neo4j.gds.core.utils.mem.ImmutableMemoryEstimationWithDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimationWithDimensions;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.internal.AuraMaintenanceSettings;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.function.Predicate.isEqual;
import static org.neo4j.gds.MemoryValidation.validateMemoryUsage;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
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

    protected BaseProc() {
        if (GdsEdition.instance().isInvalidLicense()) {
            throw new RuntimeException(GdsEdition.instance().errorMessage().get());
        }
        if (allocationTracker == null) {
            allocationTracker = AllocationTracker.empty();
        }
    }

    protected AllocationTracker allocationTracker() {
        return allocationTracker;
    }

    protected String username() {
        return transaction != null
            ? transaction.subjectOrAnonymous().username()
            : AuthSubject.ANONYMOUS.username();
    }

    protected NamedDatabaseId databaseId() {
        return api.databaseId();
    }

    protected GraphStoreWithConfig graphStoreFromCatalog(String graphName, BaseConfig config) {
        var request = catalogRequest(Optional.ofNullable(config.usernameOverride()), Optional.empty());
        return GraphStoreCatalog.get(request, graphName);
    }

    protected CatalogRequest catalogRequest(Optional<String> usernameOverride, Optional<String> databaseOverride) {
        return ImmutableCatalogRequest.of(
            databaseOverride.orElseGet(() -> databaseId().name()),
            username(),
            usernameOverride,
            isGdsAdmin()
        );
    }

    // this should be the same as the predefined role from enterprise-security
    // com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN
    private static final String PREDEFINED_ADMIN_ROLE = "admin";

    protected boolean isGdsAdmin() {
        if (transaction == null) {
            // No transaction available (likely we're in a test), no-one is admin here
            return false;
        }
        if (GdsEdition.instance().isOnCommunityEdition()) {
            // Only GDS-EE knows the concept of GDS Admins
            return false;
        }
        // only users with the admin role are GDS admins
        return transaction.securityContext().roles().contains(PREDEFINED_ADMIN_ROLE);
    }

    protected final GraphLoader newLoader(
        GraphCreateConfig createConfig,
        AllocationTracker allocationTracker,
        TaskRegistryFactory taskRegistryFactory
    ) {
        if (api == null) {
            return newFictitiousLoader(createConfig);
        }
        return ImmutableGraphLoader
            .builder()
            .context(ImmutableGraphLoaderContext.builder()
                .transactionContext(TransactionContext.of(api, procedureTransaction))
                .api(api)
                .log(log)
                .allocationTracker(allocationTracker)
                .taskRegistryFactory(taskRegistryFactory)
                .terminationFlag(TerminationFlag.wrap(transaction))
                .build())
            .username(username())
            .createConfig(createConfig)
            .build();
    }

    private GraphLoader newFictitiousLoader(GraphCreateConfig createConfig) {
        return ImmutableGraphLoader
            .builder()
            .context(GraphLoaderContext.NULL_CONTEXT)
            .username(username())
            .createConfig(createConfig)
            .build();
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

    protected <C extends BaseConfig> MemoryRange tryValidateMemoryUsage(C config, Function<C, MemoryTreeWithDimensions> runEstimation) {
        return tryValidateMemoryUsage(config, runEstimation, GcListenerExtension::freeMemory);
    }

    public <C extends BaseConfig> MemoryRange tryValidateMemoryUsage(
        C config,
        Function<C, MemoryTreeWithDimensions> runEstimation,
        FreeMemoryInspector inspector
    ) {
        MemoryTreeWithDimensions memoryTreeWithDimensions = null;

        try {
            memoryTreeWithDimensions = runEstimation.apply(config);
        } catch (MemoryEstimationNotImplementedException ignored) {
        }

        if (memoryTreeWithDimensions == null) {
            return MemoryRange.empty();
        }

        if (config.sudo()) {
            log.debug("Sudo mode: Won't check for available memory.");
        } else {
            var neo4jConfig = GraphDatabaseApiProxy.resolveDependency(api, Config.class);
            var useMaxMemoryEstimation = neo4jConfig.get(AuraMaintenanceSettings.validate_using_max_memory_estimation);
            validateMemoryUsage(memoryTreeWithDimensions, inspector.freeMemory(), useMaxMemoryEstimation);
        }

        return memoryTreeWithDimensions.memoryTree.memoryUsage();
    }

    protected MemoryEstimationWithDimensions estimateGraphCreate(GraphCreateConfig config) {
        GraphDimensions estimateDimensions;
        GraphStoreFactory<?, ?> graphStoreFactory;

        if (config.isFictitiousLoading()) {
            var labelCount = 0;
            if (config instanceof GraphCreateFromStoreConfig) {
                var storeConfig = (GraphCreateFromStoreConfig) config;
                Set<NodeLabel> nodeLabels = storeConfig.nodeProjections().projections().keySet();
                labelCount = nodeLabels.stream().allMatch(isEqual(NodeLabel.ALL_NODES)) ? 0 : nodeLabels.size();
            }

            estimateDimensions = ImmutableGraphDimensions.builder()
                .nodeCount(config.nodeCount())
                .highestNeoId(config.nodeCount())
                .estimationNodeLabelCount(labelCount)
                .relationshipCounts(Collections.singletonMap(ALL_RELATIONSHIPS, config.relationshipCount()))
                .maxRelCount(Math.max(config.relationshipCount(), 0))
                .build();

            GraphLoader loader = newLoader(config, AllocationTracker.empty(), EmptyTaskRegistryFactory.INSTANCE);
            graphStoreFactory = loader
                .createConfig()
                .graphStoreFactory()
                .getWithDimension(loader.context(), estimateDimensions);
        } else {
            GraphLoader loader = newLoader(config, AllocationTracker.empty(), EmptyTaskRegistryFactory.INSTANCE);
            graphStoreFactory = loader.graphStoreFactory();
            estimateDimensions = graphStoreFactory.estimationDimensions();
        }

        return ImmutableMemoryEstimationWithDimensions.builder()
            .memoryEstimation(graphStoreFactory.memoryEstimation())
            .graphDimensions(estimateDimensions)
            .build();
    }

    @FunctionalInterface
    public interface FreeMemoryInspector {
        long freeMemory();
    }
}
