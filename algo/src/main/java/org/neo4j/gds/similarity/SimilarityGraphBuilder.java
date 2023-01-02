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
package org.neo4j.gds.similarity;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class SimilarityGraphBuilder {

    public static MemoryEstimation memoryEstimation(int topK, int topN) {
        return MemoryEstimations.setup("", (dimensions, concurrency) -> {
            long maxNodesToCompare = Math.min(dimensions.relCountUpperBound(), dimensions.nodeCount());
            long maxNumberOfSimilarityResults = maxNodesToCompare * (maxNodesToCompare - 1) / 2;

            long newNodeCount = maxNodesToCompare;
            long newRelationshipCount = maxNumberOfSimilarityResults;

            if (topN > 0) {
                newRelationshipCount = Math.min(newRelationshipCount, topN);
                // If we reduce the number of relationships via topN,
                // we also have a new upper bound of the number of
                // nodes connected by those relationships.
                // The upper bound is a graph consisting of disjoint node pairs.
                newNodeCount = Math.min(maxNodesToCompare, newRelationshipCount * 2);
            }

            int averageDegree = Math.toIntExact(newRelationshipCount / newNodeCount);
            // For topK, we duplicate each similarity pair, which leads to a higher average degree.
            // At the same time, we limit the average degree by topK.
            if (topK > 0) {
                averageDegree = Math.min(Math.toIntExact(2 * newRelationshipCount / newNodeCount), topK);
            }

            return MemoryEstimations.builder(HugeGraph.class)
                .add(
                    "adjacency list",
                    AdjacencyListBehavior.adjacencyListEstimation(averageDegree, newNodeCount)
                )
                .build();
        });
    }

    private final IdMap idMap;
    private final int concurrency;
    private final ExecutorService executorService;

    public SimilarityGraphBuilder(
        IdMap idMap,
        int concurrency,
        ExecutorService executorService
    ) {
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.idMap = idMap;
    }

    public Graph build(Stream<SimilarityResult> stream) {
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap.rootIdMap())
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of("property"))
            .concurrency(concurrency)
            .executorService(executorService)
            .build();

        ParallelUtil.parallelStreamConsume(
            stream,
            concurrency,
            similarityStream -> similarityStream.forEach(similarityResult -> relationshipsBuilder.addFromInternal(
                idMap.toRootNodeId(similarityResult.sourceNodeId()),
                idMap.toRootNodeId(similarityResult.targetNodeId()),
                similarityResult.similarity
            ))
        );

        return GraphFactory.create(
            idMap.rootIdMap(),
            relationshipsBuilder.build()
        );
    }
}
