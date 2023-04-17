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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.packed.PackedCompressor;
import org.neo4j.gds.core.compression.uncompressed.RawCompressor;
import org.neo4j.gds.core.compression.uncompressed.UncompressedAdjacencyList;
import org.neo4j.gds.core.compression.uncompressed.UncompressedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.compression.varlong.CompressedAdjacencyList;
import org.neo4j.gds.core.compression.varlong.CompressedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.compression.varlong.DeltaVarLongCompressor;
import org.neo4j.gds.core.compression.packed.PackedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;

/**
 * Manages different configurations of adjacency list building,
 * i.e., compressed or uncompressed.
 */
public interface AdjacencyListBehavior {

    static AdjacencyCompressorFactory asConfigured(
        LongSupplier nodeCountSupplier,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations
    ) {
        var resolvedAggregations = Arrays.stream(aggregations).map(Aggregation::resolve).toArray(Aggregation[]::new);
        var noAggregation = Arrays.stream(aggregations).map(Aggregation::resolve).allMatch(Aggregation::equivalentToNone);

        return GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.isEnabled()
            ? packed(nodeCountSupplier, propertyMappings, resolvedAggregations, noAggregation)
            : GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
                ? uncompressed(nodeCountSupplier, propertyMappings, resolvedAggregations, noAggregation)
                : compressed(nodeCountSupplier, propertyMappings, resolvedAggregations, noAggregation);
    }

    static AdjacencyCompressorFactory compressed(
        LongSupplier nodeCountSupplier,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        return DeltaVarLongCompressor.factory(
            nodeCountSupplier,
            CompressedAdjacencyListBuilderFactory.of(),
            propertyMappings,
            aggregations,
            noAggregation
        );
    }

    static AdjacencyCompressorFactory uncompressed(
        LongSupplier nodeCountSupplier,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        return RawCompressor.factory(
            nodeCountSupplier,
            UncompressedAdjacencyListBuilderFactory.of(),
            propertyMappings,
            aggregations,
            noAggregation
        );
    }

    static AdjacencyCompressorFactory packed(
        LongSupplier nodeCountSupplier,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        return PackedCompressor.factory(
            nodeCountSupplier,
            PackedAdjacencyListBuilderFactory.of(),
            propertyMappings,
            aggregations,
            noAggregation
        );
    }

    static MemoryEstimation adjacencyListEstimation(long avgDegree, long nodeCount) {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? UncompressedAdjacencyList.adjacencyListEstimation(avgDegree, nodeCount)
            : CompressedAdjacencyList.adjacencyListEstimation(avgDegree, nodeCount);
    }

    static MemoryEstimation adjacencyListEstimation(RelationshipType relationshipType, boolean undirected) {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? UncompressedAdjacencyList.adjacencyListEstimation(relationshipType, undirected)
            : CompressedAdjacencyList.adjacencyListEstimation(relationshipType, undirected);
    }

    static MemoryEstimation adjacencyListsFromStarEstimation(boolean undirected) {
        BiFunction<RelationshipType, Boolean, MemoryEstimation> estimationMethod = GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? UncompressedAdjacencyList::adjacencyListEstimation
            : CompressedAdjacencyList::adjacencyListEstimation;

        return MemoryEstimations.setup("Adjacency Lists", dimensions -> {
            var builder = MemoryEstimations.builder();

            if (dimensions.relationshipCounts().isEmpty()) {
                builder.add(adjacencyListEstimation(ALL_RELATIONSHIPS, undirected));
            } else {
                dimensions
                    .relationshipCounts()
                    .forEach((type, count) -> builder.add(type.name, estimationMethod.apply(type, undirected)));
            }

            return builder.build();
        });
    }

    static MemoryEstimation adjacencyPropertiesEstimation(RelationshipType relationshipType, boolean undirected) {
        return UncompressedAdjacencyList.adjacencyPropertiesEstimation(relationshipType, undirected);
    }

    static MemoryEstimation adjacencyPropertiesFromStarEstimation(boolean undirected) {
        return MemoryEstimations.setup("", dimensions -> {
            var builder = MemoryEstimations.builder();

            if (dimensions.relationshipCounts().isEmpty()) {
                builder.add(UncompressedAdjacencyList.adjacencyPropertiesEstimation(ALL_RELATIONSHIPS, undirected));
            } else {
                dimensions
                    .relationshipCounts()
                    .forEach((type, count) -> builder.add(
                        type.name,
                        UncompressedAdjacencyList.adjacencyPropertiesEstimation(
                            type,
                            undirected
                        )
                    ));
            }

            return builder.build();
        });
    }
}
