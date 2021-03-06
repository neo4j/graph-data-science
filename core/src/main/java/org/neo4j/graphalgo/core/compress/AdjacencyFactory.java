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
package org.neo4j.graphalgo.core.compress;

import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.TransientCompressedList;
import org.neo4j.graphalgo.core.huge.TransientUncompressedList;
import org.neo4j.graphalgo.core.loading.DeltaVarLongCompressor;
import org.neo4j.graphalgo.core.loading.RawCompressor;
import org.neo4j.graphalgo.core.loading.TransientCompressedCsrListBuilderFactory;
import org.neo4j.graphalgo.core.loading.TransientUncompressedCsrListBuilderFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;

import java.util.stream.Stream;

public interface AdjacencyFactory {

    AdjacencyCompressorBlueprint create(
        long nodeCount,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation,
        AllocationTracker tracker
    );

    default AdjacencyCompressorBlueprint create(
        long nodeCount,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        AllocationTracker tracker
    ) {
        return create(
            nodeCount,
            propertyMappings,
            aggregations,
            Stream.of(aggregations).allMatch(aggregation -> aggregation == Aggregation.NONE),
            tracker
        );
    }

    static AdjacencyFactory configured() {
        return GdsFeatureToggles.USE_UNCOMPRESSED_ADJACENCY_LIST.isEnabled()
            ? transientUncompressed()
            : transientCompressed();
    }

    static AdjacencyFactory transientCompressed() {
        return (nodeCount, propertyMappings, aggregations, noAggregation, tracker) ->
            DeltaVarLongCompressor.Factory.INSTANCE.create(
                nodeCount,
                TransientCompressedCsrListBuilderFactory.of(tracker),
                propertyMappings,
                aggregations,
                noAggregation,
                tracker
            );
    }

    static AdjacencyFactory transientUncompressed() {
        return (nodeCount, propertyMappings, aggregations, noAggregation, tracker) ->
            RawCompressor.Factory.INSTANCE.create(
                nodeCount,
                TransientUncompressedCsrListBuilderFactory.of(tracker),
                propertyMappings,
                aggregations,
                noAggregation,
                tracker
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
