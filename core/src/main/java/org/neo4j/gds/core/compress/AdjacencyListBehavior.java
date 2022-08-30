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
package org.neo4j.gds.core.compress;

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.CompressedAdjacencyList;
import org.neo4j.gds.core.huge.UncompressedAdjacencyList;
import org.neo4j.gds.core.loading.CompressedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.loading.DeltaVarLongCompressor;
import org.neo4j.gds.core.loading.RawCompressor;
import org.neo4j.gds.core.loading.UncompressedAdjacencyListBuilderFactory;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.util.Arrays;
import java.util.function.LongSupplier;

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

        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
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

    static MemoryEstimation adjacencyPropertiesEstimation(RelationshipType relationshipType, boolean undirected) {
        return UncompressedAdjacencyList.adjacencyPropertiesEstimation(relationshipType, undirected);
    }
}
