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
package org.neo4j.gds.executor;

import org.jetbrains.annotations.Nullable;
import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.Optional;

/**
 * A lovely mish-mash of long-lived services, request scoped services, and parameters. Embrace it.
 * And by that I mean, stop thinking and keep abusing this non-design.
 */
public record ExecutionContext(
    CloseableResourceRegistry closeableResourceRegistry,
    DatabaseId databaseId,
    Log log,
    MemoryEstimationContext memoryEstimationContext,
    Metrics metrics,
    ProcedureReturnColumns returnColumns,
    RequestCorrelationId requestCorrelationId,
    TaskRegistryFactory taskRegistryFactory,
    TerminationMonitor terminationMonitor,
    User user,
    @Nullable AlgorithmsProcedureFacade algorithmsProcedureFacade,
    @Nullable DependencyResolver dependencyResolver,
    @Nullable ModelCatalog modelCatalog,
    @Nullable NodePropertyExporterBuilder nodePropertyExporterBuilder,
    @Nullable RelationshipExporterBuilder relationshipExporterBuilder
) {

    public ExecutionContext(
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        Log log,
        MemoryEstimationContext memoryEstimationContext,
        Metrics metrics,
        ProcedureReturnColumns returnColumns,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        User user,
        @Nullable AlgorithmsProcedureFacade algorithmsProcedureFacade,
        @Nullable ModelCatalog modelCatalog,
        @Nullable NodePropertyExporterBuilder nodePropertyExporterBuilder,
        @Nullable RelationshipExporterBuilder relationshipExporterBuilder
    ) {
        this(closeableResourceRegistry, databaseId, log, memoryEstimationContext, metrics, returnColumns, requestCorrelationId, taskRegistryFactory, terminationMonitor, user, algorithmsProcedureFacade, EMPTY_DEPENDENCY_RESOLVER, modelCatalog, nodePropertyExporterBuilder, relationshipExporterBuilder);
    }

    public ExecutionContext(
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        Log log,
        MemoryEstimationContext memoryEstimationContext,
        Metrics metrics,
        ProcedureReturnColumns returnColumns,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        User user,
        @Nullable AlgorithmsProcedureFacade algorithmsProcedureFacade,
        @Nullable DependencyResolver dependencyResolver,
        @Nullable ModelCatalog modelCatalog
    ) {
        this(closeableResourceRegistry, databaseId, log, memoryEstimationContext, metrics, returnColumns, requestCorrelationId, taskRegistryFactory, terminationMonitor, user, algorithmsProcedureFacade, dependencyResolver, modelCatalog, null, null);
    }
    public ExecutionContext withNodePropertyExporterBuilder(NodePropertyExporterBuilder nodePropertyExporterBuilder) {
        return new ExecutionContext(
            closeableResourceRegistry,
            databaseId,
            log,
            memoryEstimationContext,
            metrics,
            returnColumns,
            requestCorrelationId,
            taskRegistryFactory,
            terminationMonitor,
            user,
            algorithmsProcedureFacade,
            dependencyResolver,
            modelCatalog,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder
        );
    }

    public ExecutionContext withModelCatalog(ModelCatalog modelCatalog) {
        return new ExecutionContext(
            closeableResourceRegistry,
            databaseId,
            log,
            memoryEstimationContext,
            metrics,
            returnColumns,
            requestCorrelationId,
            taskRegistryFactory,
            terminationMonitor,
            user,
            algorithmsProcedureFacade,
            dependencyResolver,
            modelCatalog,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder
        );
    }

    public static final DependencyResolver EMPTY_DEPENDENCY_RESOLVER = new DependencyResolver() {
        @Override
        public <T> Optional<T> resolveOptionalDependency(Class<T> aClass) {
            return Optional.empty();
        }

        @Override
        public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
            return null;
        }

        @Override
        public boolean containsDependency(Class<?> type) {
            return false;
        }
    };

    public static final MemoryEstimationContext EMPTY_MEMORY_CONTEXT = new MemoryEstimationContext(false);

    public static final ExecutionContext EMPTY = new ExecutionContext(
        CloseableResourceRegistry.EMPTY,
        DatabaseId.EMPTY,
        Log.noOpLog(),
        EMPTY_MEMORY_CONTEXT,
        Metrics.DISABLED,
        ProcedureReturnColumns.EMPTY,
        PlainSimpleRequestCorrelationId.create(),
        EmptyTaskRegistryFactory.INSTANCE,
        TerminationMonitor.EMPTY,
        new User("", false),
        null,
        EMPTY_DEPENDENCY_RESOLVER,
        ModelCatalog.EMPTY,
        null,
        null
    );
}
