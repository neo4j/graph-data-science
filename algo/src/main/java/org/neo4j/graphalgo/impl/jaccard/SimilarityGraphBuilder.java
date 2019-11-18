/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.impl.jaccard;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.RelationshipStreamBuilder;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

class SimilarityGraphBuilder {

    static MemoryEstimation memoryEstimation(int topK, int top) {
        return MemoryEstimations.setup("", (dimensions, concurrency) -> {
            long maxNodesToCompare = Math.min(dimensions.maxRelCount(), dimensions.nodeCount());
            long maxNumberOfSimilarityResults = maxNodesToCompare * (maxNodesToCompare - 1) / 2;
            long maxNodesWithNewRels = maxNodesToCompare;
            if (top > 0) {
                maxNumberOfSimilarityResults = Math.min(maxNumberOfSimilarityResults, top);
                maxNodesWithNewRels = maxNumberOfSimilarityResults * 2;
            }
            int averageDegree = Math.toIntExact(maxNumberOfSimilarityResults / maxNodesWithNewRels);
            if (topK > 0) {
                averageDegree = Math.min(averageDegree, topK);
            }
            return MemoryEstimations.builder(HugeGraph.class)
                .add(
                    "adjacency list",
                    AdjacencyList.compressedMemoryEstimation(averageDegree, maxNodesWithNewRels)
                )
                .add("adjacency offsets", AdjacencyOffsets.memoryEstimation())
                .build();
        });
    }

    private final HugeGraph baseGraph;
    private final RelationshipStreamBuilder relationshipStreamBuilder;

    SimilarityGraphBuilder(Graph baseGraph, long nodesToCompare, ExecutorService executorService, AllocationTracker tracker) {
        if (baseGraph instanceof HugeGraph) {
            this.baseGraph = (HugeGraph) baseGraph;
        } else {
            throw new IllegalArgumentException("Base graph must be a huge graph.");
        }

        this.relationshipStreamBuilder = new RelationshipStreamBuilder(baseGraph, nodesToCompare, executorService, tracker);
    }

    Graph build(Stream<SimilarityResult> stream) {
        return HugeGraph.create(baseGraph, relationshipStreamBuilder.loadRelationships(stream), false);
    }
}
