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

package org.neo4j.graphalgo;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphLoader;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
final class GraphLoaderBuilders {

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
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker,
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
        @Builder.Switch(defaultName = "PROJECTION") GraphCreateConfigBuilders.AnyLabel anyLabel,
        @Builder.Switch(defaultName = "PROJECTION") GraphCreateConfigBuilders.AnyRelationshipType anyRelationshipType,
        Optional<Orientation> globalOrientation,
        Optional<Aggregation> globalAggregation,
        Optional<Boolean> throwOnUnresolvedRelationships
        ) {

        GraphCreateFromStoreConfig graphCreateConfig = GraphCreateConfigBuilders.storeConfig(
            userName,
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
            anyLabel,
            anyRelationshipType,
            globalOrientation,
            globalAggregation,
            throwOnUnresolvedRelationships
        );

        return createGraphLoader(api, executorService, tracker, terminationFlag, log, userName, graphCreateConfig);
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
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        // CreateConfig parameters
        Optional<String> graphName,
        Optional<String> nodeQuery,
        Optional<String> relationshipQuery,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relationshipProperties,
        @Builder.Switch(defaultName = "PROJECTION") GraphCreateConfigBuilders.AnyLabel anyLabel,
        @Builder.Switch(defaultName = "PROJECTION") GraphCreateConfigBuilders.AnyRelationshipType anyRelationshipType,
        Optional<Integer> concurrency,
        Optional<Aggregation> globalAggregation,
        Optional<Boolean> throwOnUnresolvedRelationships,
        Optional<Map<String, Object>> parameters
    ) {
        GraphCreateFromCypherConfig graphCreateConfig = GraphCreateConfigBuilders.cypherConfig(
            userName,
            graphName,
            nodeQuery,
            relationshipQuery,
            nodeProperties,
            relationshipProperties,
            anyLabel,
            anyRelationshipType,
            concurrency,
            globalAggregation,
            throwOnUnresolvedRelationships,
            parameters
        );

        return createGraphLoader(api, executorService, tracker, terminationFlag, log, userName, graphCreateConfig);
    }

    @NotNull
    private static GraphLoader createGraphLoader(
        GraphDatabaseAPI api,
        Optional<ExecutorService> executorService,
        Optional<AllocationTracker> tracker,
        Optional<TerminationFlag> terminationFlag,
        Optional<Log> log,
        Optional<String> userName,
        GraphCreateConfig graphCreateConfig
    ) {
        return ImmutableGraphLoader.of(
            api,
            executorService.orElse(Pools.DEFAULT),
            tracker.orElse(AllocationTracker.EMPTY),
            terminationFlag.orElse(TerminationFlag.RUNNING_TRUE),
            userName.orElse(""),
            log.orElse(NullLog.getInstance()),
            graphCreateConfig
        );
    }
}
