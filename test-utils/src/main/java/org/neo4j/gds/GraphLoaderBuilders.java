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

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.GraphCreateFromCypherConfig;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.ImmutableGraphLoader;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class GraphLoaderBuilders {

    private GraphLoaderBuilders() { }

    /**
     * Factory method that defines the generation of a {@link GraphLoader}
     * using a {@link GraphCreateFromStoreConfig}. Use {@link StoreLoaderBuilder}
     * to create the input for that method in a convenient way.
     */
    @Builder.Factory
    static GraphLoader storeLoader(
        // GraphLoader parameters
        GraphDatabaseAPI api,
        Optional<TransactionContext> transactionContext,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> allocationTracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        // CreateConfig parameters
        Optional<String> graphName,
        List<String> nodeLabels,
        List<String> relationshipTypes,
        List<NodeProjection> nodeProjections,
        List<RelationshipProjection> relationshipProjections,
        Map<String, NodeProjection> nodeProjectionsWithIdentifier,
        Map<String, RelationshipProjection> relationshipProjectionsWithIdentifier,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relationshipProperties,
        Optional<Integer> concurrency,
        Optional<Orientation> globalOrientation,
        Optional<Aggregation> globalAggregation,
        Optional<Boolean> validateRelationships
    ) {

        GraphCreateFromStoreConfig graphCreateConfig = GraphCreateConfigBuilders.storeConfig(
            userName.or(() -> transactionContext.map(TransactionContext::username)),
            graphName,
            nodeLabels,
            relationshipTypes,
            nodeProjections,
            relationshipProjections,
            nodeProjectionsWithIdentifier,
            relationshipProjectionsWithIdentifier,
            nodeProperties,
            relationshipProperties,
            concurrency,
            globalOrientation,
            globalAggregation,
            validateRelationships
        );

        return createGraphLoader(
            api,
            transactionContext,
            executorService,
            allocationTracker,
            terminationFlag,
            log,
            userName,
            graphCreateConfig
        );
    }

    @Builder.Factory
    static GraphLoader storeLoaderWithConfig(
        // GraphLoader parameters
        GraphDatabaseAPI api,
        Optional<TransactionContext> transactionContext,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> allocationTracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        GraphCreateFromStoreConfig graphCreateConfig
    ) {
        return createGraphLoader(
            api,
            transactionContext,
            executorService,
            allocationTracker,
            terminationFlag,
            log,
            userName,
            graphCreateConfig
        );
    }

    /**
     * Factory method that defines the generation of a {@link GraphLoader}
     * using a {@link GraphCreateFromCypherConfig}. Use {@link CypherLoaderBuilder}
     * to create the input for that method in a convenient way.
     */
    @Builder.Factory
    static GraphLoader cypherLoader(
        // GraphLoader parameters
        GraphDatabaseAPI api,
        Optional<TransactionContext> transactionContext,
        Optional<AllocationTracker> allocationTracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        // CreateConfig parameters
        Optional<String> graphName,
        Optional<String> nodeQuery,
        Optional<String> relationshipQuery,
        Optional<Integer> concurrency,
        Optional<Boolean> validateRelationships,
        Optional<Map<String, Object>> parameters
    ) {
        GraphCreateFromCypherConfig graphCreateConfig = GraphCreateConfigBuilders.cypherConfig(
            userName.or(() -> transactionContext.map(TransactionContext::username)),
            graphName,
            nodeQuery,
            relationshipQuery,
            concurrency,
            validateRelationships,
            parameters
        );

        return createGraphLoader(
            api,
            transactionContext,
            Optional.empty(),
            allocationTracker,
            terminationFlag,
            log,
            userName,
            graphCreateConfig
        );
    }

    @NotNull
    public static GraphLoader createGraphLoader(
        GraphDatabaseAPI api,
        Optional<TransactionContext> transactionContext,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> allocationTracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        GraphCreateConfig graphCreateConfig
    ) {
        return ImmutableGraphLoader.builder()
            .context(ImmutableGraphLoaderContext.builder()
                .transactionContext(transactionContext.orElseGet(() -> TestSupport.fullAccessTransaction(api)))
                .api(api)
                .executor(executorService.orElse(Pools.DEFAULT))
                .allocationTracker(allocationTracker.orElse(AllocationTracker.empty()))
                .terminationFlag(terminationFlag.orElse(TerminationFlag.RUNNING_TRUE))
                .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
                .log(log.orElse(NullLog.getInstance()))
                .build())
            .username(userName
                .or(() -> transactionContext.map(TransactionContext::username))
                .orElse(""))
            .createConfig(graphCreateConfig)
            .build();
    }
}
