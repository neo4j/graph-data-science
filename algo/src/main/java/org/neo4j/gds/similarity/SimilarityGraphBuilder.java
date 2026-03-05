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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.algorithms.similarity.SimilaritySummaryBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.AdjacencyListBehavior;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.TopKGraph;
import org.neo4j.gds.similarity.nodesim.TopKMap;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class SimilarityGraphBuilder {

    public static MemoryEstimation memoryEstimation(int topK, int topN) {
        return MemoryEstimations.setup(
            "", (dimensions, concurrency) -> {
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
            }
        );
    }

    private final Graph graph;
    private final TerminationFlag terminationFlag;
    private final Concurrency concurrency;
    private final ExecutorService executorService;
    private final boolean shouldConstructDistribution;

    public SimilarityGraphBuilder(
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        TerminationFlag terminationFlag,
        boolean shouldConstructDistribution
    ) {
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.graph = graph;
        this.terminationFlag = terminationFlag;
        this.shouldConstructDistribution = shouldConstructDistribution;
    }

    public SimilarityGraph build(Stream<SimilarityResult> stream) {
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(graph.rootIdMap())
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.NATURAL)
            .addPropertyConfig(GraphFactory.PropertyConfig.of("property"))
            .concurrency(concurrency)
            .executorService(executorService)
            .build();

        var similaritySummaryBuilder = SimilaritySummaryBuilder.of(concurrency, shouldConstructDistribution);

        ParallelUtil.parallelStreamConsume(
            stream,
            concurrency,
            terminationFlag,
            similarityStream -> similarityStream.forEach(similarityResult -> {
                relationshipsBuilder.addFromInternal(
                    graph.toRootNodeId(similarityResult.sourceNodeId()),
                    graph.toRootNodeId(similarityResult.targetNodeId()),
                    similarityResult.similarity
                );
                similaritySummaryBuilder.accept(
                    similarityResult.sourceNodeId(),
                    similarityResult.targetNodeId(),
                    similarityResult.similarity
                );

            })
        );

        var similarityGraph = GraphFactory.create(
            graph.rootIdMap(),
            relationshipsBuilder.build()
        );
        return new HugeSimilarityGraph(
            similarityGraph,
            similaritySummaryBuilder.similaritySummary()
        );
    }

    public SimilarityGraph build(TopKMap topKMap) {
        return new TopKSimilarityGraph(
            new TopKGraph(graph, topKMap),
            shouldConstructDistribution
        );
    }

    public SimilarityGraph build(NodeSimilarityResult nodeSimilarityResult){
        if (nodeSimilarityResult.maybeStreamResult().isPresent()) {
            return build(nodeSimilarityResult.streamResult());
        }
        return  build(nodeSimilarityResult.topKMap());
    }
}
