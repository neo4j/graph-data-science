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

import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.TransientCompressedList;
import org.neo4j.gds.core.huge.TransientUncompressedList;
import org.neo4j.gds.utils.GdsFeatureToggles;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.DeltaVarLongCompressor;
import org.neo4j.gds.core.loading.RawCompressor;
import org.neo4j.gds.core.loading.TransientCompressedCsrListBuilderFactory;
import org.neo4j.gds.core.loading.TransientUncompressedCsrListBuilderFactory;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;

import java.util.stream.Stream;

public interface AdjacencyFactory {

    AdjacencyCompressorBlueprint create(
        long nodeCount,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation,
        AllocationTracker allocationTracker
    );

    default AdjacencyCompressorBlueprint create(
        long nodeCount,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        AllocationTracker allocationTracker
    ) {
        return create(
            nodeCount,
            propertyMappings,
            aggregations,
            Stream.of(aggregations).allMatch(aggregation -> aggregation == Aggregation.NONE),
            allocationTracker
        );
    }

    static AdjacencyFactory configured() {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? transientUncompressed()
            : transientCompressed();
    }

    static AdjacencyFactory transientCompressed() {
        return (nodeCount, propertyMappings, aggregations, noAggregation, allocationTracker) ->
            DeltaVarLongCompressor.Factory.INSTANCE.create(
                nodeCount,
                TransientCompressedCsrListBuilderFactory.of(allocationTracker),
                propertyMappings,
                aggregations,
                noAggregation,
                allocationTracker
            );
    }

    static AdjacencyFactory transientUncompressed() {
        return (nodeCount, propertyMappings, aggregations, noAggregation, allocationTracker) ->
            RawCompressor.Factory.INSTANCE.create(
                nodeCount,
                TransientUncompressedCsrListBuilderFactory.of(allocationTracker),
                propertyMappings,
                aggregations,
                noAggregation,
                allocationTracker
            );
    }

    static MemoryEstimation adjacencyListEstimation(long avgDegree, long nodeCount) {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? TransientUncompressedList.adjacencyListEstimation(avgDegree, nodeCount)
            : TransientCompressedList.adjacencyListEstimation(avgDegree, nodeCount);
    }

    static MemoryEstimation adjacencyListEstimation(RelationshipType relationshipType, boolean undirected) {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? TransientUncompressedList.adjacencyListEstimation(relationshipType, undirected)
            : TransientCompressedList.adjacencyListEstimation(relationshipType, undirected);
    }

    static MemoryEstimation adjacencyPropertiesEstimation(RelationshipType relationshipType, boolean undirected) {
        return TransientUncompressedList.adjacencyPropertiesEstimation(relationshipType, undirected);
    }
}
