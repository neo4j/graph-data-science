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
package org.neo4j.gds.ml.kge;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.predicates.LongLongPredicate;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.similarity.nodesim.TopKMap;

import java.util.List;
import java.util.stream.LongStream;

public class TopKMapComputer extends Algorithm<KGEPredictResult> {

    private Graph graph;
    private ProgressTracker progressTracker;
    private BitSet sourceNodes;
    private BitSet targetNodes;

    private String nodeEmbeddingProperty;
    private List<Double> relationshipTypeEmbedding;
    private int concurrency;

    private int topK;
    private String scoreFunction;

    // TODO abstract / merge with LinkFilter?
    private LongLongPredicate isCandidateLink;

    public TopKMapComputer(
        Graph graph,
        BitSet sourceNodes,
        BitSet targetNodes,
        String nodeEmbeddingProperty,
        List<Double> relationshipTypeEmbedding,
        String scoreFunction,
        LongLongPredicate isCandidateLink,
        int topK,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.progressTracker = progressTracker;
        this.sourceNodes = sourceNodes;
        this.targetNodes = targetNodes;
        this.nodeEmbeddingProperty = nodeEmbeddingProperty;
        this.relationshipTypeEmbedding = relationshipTypeEmbedding;
        this.concurrency = concurrency;
        this.topK = topK;
        this.scoreFunction = scoreFunction;
        this.isCandidateLink = isCandidateLink;
    }

    public KGEPredictResult compute() {
        progressTracker.beginSubTask(estimateWorkload());

        TopKMap topKMap = new TopKMap(sourceNodes.capacity(), sourceNodes, Math.abs(topK), true);

        NodePropertyValues embeddings = graph.nodeProperties(nodeEmbeddingProperty);

        try (var threadLocalSimilarityComputer = AutoCloseableThreadLocal.withInitial(() -> LinkScorerFactory.create(scoreFunction))) {
            // TODO exploit symmetry of similarity function if available
            ParallelUtil.parallelStreamConsume(
                new SetBitsIterable(sourceNodes).stream(),
                concurrency,
                terminationFlag,
                stream -> stream
                    .forEach(node1 -> {
                        terminationFlag.assertRunning();

                        LinkScorer similarityComputer = threadLocalSimilarityComputer.get();
                        similarityComputer.init(embeddings, relationshipTypeEmbedding, node1);

                        targetNodesStream()
                            .filter(node2 -> isCandidateLink.apply(node1, node2))
                            .forEach(node2 -> {
                                double similarity = similarityComputer.similarity(node2);
                                if (!Double.isNaN(similarity)) {
                                    topKMap.put(node1, node2, similarity);
                                }
                                progressTracker.logProgress();
                            });
                    })
            );
        }

        progressTracker.endSubTask();

        return KGEPredictResult.of(topKMap);
    }


    private LongStream targetNodesStream() {
        return new SetBitsIterable(targetNodes, 0).stream();
    }

    private long estimateWorkload() {
        return sourceNodes.cardinality() * targetNodes.cardinality();
    }
}
