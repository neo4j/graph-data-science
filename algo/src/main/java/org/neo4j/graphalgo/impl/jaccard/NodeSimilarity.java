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

import com.carrotsearch.hppc.ArraySizingStrategy;
import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Direction;

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class NodeSimilarity extends Algorithm<NodeSimilarity> {

    private final Graph graph;
    private final Config config;

    private final ExecutorService executorService;
    private final AllocationTracker tracker;

    private final BitSet nodeFilter;

    private HugeObjectArray<long[]> vectors;
    private long nodesToCompare;

    public NodeSimilarity(
        Graph graph,
        Config config,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.executorService = executorService;
        this.tracker = tracker;
        this.nodeFilter = new BitSet(graph.nodeCount());
    }

    @Override
    public NodeSimilarity me() {
        return this;
    }

    @Override
    public void release() {
        graph.release();
    }

    // The buffer is sized on the first call to the sizing strategy to hold exactly node degree elements
    private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
        (currentBufferLength, elementsCount, degree) -> elementsCount + degree;

    public Stream<SimilarityResult> computeToStream(Direction direction) {

        // Create a filter for which nodes to compare and calculate the neighborhood for each node
        prepare(direction);

        assertRunning();

        // Compute similarities
        if (config.hasTopN() && !config.hasTopK()) {
            // Special case: compute topN without topK.
            // This can not happen when algo is called from proc.
            // Ignore parallelism, always run single threaded,
            // but run on primitives.
            return computeTopN();
        } else {
            return config.isParallel()
                ? computeParallel()
                : compute();
        }
    }

    public SimilarityGraphResult computeToGraph(Direction direction) {
        Graph similarityGraph;
        if (config.hasTopK() && !config.hasTopN()) {
            prepare(direction);
            TopKMap topKMap = config.isParallel()
                ? computeTopKMapParallel()
                : computeTopkMap();
            similarityGraph = new TopKGraph(graph, topKMap);
        } else {
            Stream<SimilarityResult> similarities = computeToStream(direction);
            similarityGraph = new SimilarityGraphBuilder(graph, nodesToCompare, executorService, tracker).build(similarities);
        }
        return new SimilarityGraphResult(similarityGraph, nodesToCompare);
    }

    private void prepare(Direction direction) {
        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                "Direction BOTH is not supported by the NodeSimilarity algorithm.");
        }

        vectors = HugeObjectArray.newArray(long[].class, graph.nodeCount(), tracker);

        DegreeComputer degreeComputer = new DegreeComputer();
        VectorComputer vectorComputer = new VectorComputer();
        vectors.setAll(node -> {
            graph.forEachRelationship(node, direction, degreeComputer);
            int degree = degreeComputer.degree;
            degreeComputer.reset();
            vectorComputer.reset(degree);
            if (degree >= config.degreeCutoff) {
                nodesToCompare++;
                nodeFilter.set(node);

                graph.forEachRelationship(node, direction, vectorComputer);
                return vectorComputer.targetIds.buffer;
            }
            return null;
        });
    }

    private Stream<SimilarityResult> compute() {
        return (config.hasTopK() && config.hasTopN())
            ? computeTopN(computeTopkMap())
            : (config.hasTopK())
                ? computeTopkMap().stream()
                : computeAll();
    }

    private Stream<SimilarityResult> computeParallel() {
        return (config.hasTopK() && config.hasTopN())
            ? computeTopN(computeTopKMapParallel())
            : (config.hasTopK())
                ? computeTopKMapParallel().stream()
                : computeAllParallel();
    }

    private Stream<SimilarityResult> computeAll() {
        return loggelableAndTerminatableNodeStream()
            .boxed()
            .flatMap(node1 -> {
                long[] vector1 = vectors.get(node1);
                return nodeStream(node1 + 1)
                    .mapToObj(node2 -> {
                        double similarity = jaccard(vector1, vectors.get(node2));
                        return Double.isNaN(similarity) ? null : new SimilarityResult(node1, node2, similarity);
                    })
                    .filter(Objects::nonNull);
            });
    }

    private Stream<SimilarityResult> computeAllParallel() {
        return ParallelUtil.parallelStream(
            loggelableAndTerminatableNodeStream(), stream -> stream
                .boxed()
                .flatMap(node1 -> {
                    long[] vector1 = vectors.get(node1);
                    return nodeStream(node1 + 1)
                        .mapToObj(node2 -> {
                            double similarity = jaccard(vector1, vectors.get(node2));
                            return Double.isNaN(similarity) ? null : new SimilarityResult(node1, node2, similarity);
                        })
                        .filter(Objects::nonNull);
                })
        );
    }

    private TopKMap computeTopkMap() {
        Comparator<SimilarityResult> comparator = config.topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), nodeFilter, Math.abs(config.topK), comparator, tracker);
        loggelableAndTerminatableNodeStream()
            .forEach(node1 -> {
                long[] vector1 = vectors.get(node1);
                nodeStream(node1 + 1)
                    .forEach(node2 -> {
                        double similarity = jaccard(vector1, vectors.get(node2));
                        if (!Double.isNaN(similarity)) {
                            topKMap.put(node1, node2, similarity);
                            topKMap.put(node2, node1, similarity);
                        }
                    });
            });
        return topKMap;
    }

    private TopKMap computeTopKMapParallel() {
        Comparator<SimilarityResult> comparator = config.topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), nodeFilter, Math.abs(config.topK), comparator, tracker);
        ParallelUtil.parallelStreamConsume(
            loggelableAndTerminatableNodeStream(),
            stream -> stream
                .forEach(node1 -> {
                    long[] vector1 = vectors.get(node1);
                    // We deliberately compute the full matrix (except the diagonal).
                    // The parallel workload is partitioned based on the outer stream.
                    // The TopKMap stores a priority queue for each node. Writing
                    // into these queues is not considered to be thread-safe.
                    // Hence, we need to ensure that down the stream, exactly one queue
                    // within the TopKMap processes all pairs for a single node.
                    nodeStream()
                        .filter(node2 -> node1 != node2)
                        .forEach(node2 -> {
                            double similarity = jaccard(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topKMap.put(node1, node2, similarity);
                            }
                        });
                })
        );
        return topKMap;
    }

    private Stream<SimilarityResult> computeTopN() {
        TopNList topNList = new TopNList(config.topN);
        loggelableAndTerminatableNodeStream()
            .forEach(node1 -> {
                long[] vector1 = vectors.get(node1);
                nodeStream(node1 + 1)
                    .forEach(node2 -> {
                        double similarity = jaccard(vector1, vectors.get(node2));
                        if (!Double.isNaN(similarity)) {
                            topNList.add(node1, node2, similarity);
                        }
                    });
            });
        return topNList.stream();
    }

    private Stream<SimilarityResult> computeTopN(TopKMap topKMap) {
        TopNList topNList = new TopNList(config.topN);
        topKMap.forEach(topNList::add);
        return topNList.stream();
    }

    private LongStream log(LongStream stream) {
        long logInterval = Math.max(1, BitUtil.nearbyPowerOfTwo(nodesToCompare / 100));
        return stream.peek(node -> {
            if ((node & (logInterval - 1)) == 0) {
                progressLogger.logProgress(node, nodesToCompare);
            }
        });
    }

    private LongStream assertRunning(LongStream stream) {
        long checkInterval = Math.max(1, BitUtil.nearbyPowerOfTwo(nodesToCompare / 100));
        return stream.peek(node -> {
            if ((node & (checkInterval - 1)) == 0) {
                assertRunning();
            }
        });
    }

    private double jaccard(long[] vector1, long[] vector2) {
        long intersection = Intersections.intersection3(vector1, vector2);
        double union = vector1.length + vector2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / union;
        return similarity >= config.similarityCutoff ? similarity : Double.NaN;
    }

    private LongStream nodeStream() {
        return nodeStream(0);
    }

    private LongStream loggelableAndTerminatableNodeStream() {
        return log(assertRunning(nodeStream()));
    }

    private LongStream nodeStream(long offset) {
        return new SetBitsIterable(nodeFilter, offset).stream();
    }

    public static final class Config {

        private final double similarityCutoff;
        private final int degreeCutoff;

        private final int topN;
        private final int topK;

        private final int concurrency;
        private final int minBatchSize;

        public Config(
            double similarityCutoff,
            int degreeCutoff,
            int topN,
            int topK,
            int concurrency,
            int minBatchSize
        ) {
            this.similarityCutoff = similarityCutoff;
            // TODO: make this constraint more prominent
            this.degreeCutoff = Math.max(1, degreeCutoff);
            this.topN = topN;
            this.topK = topK;
            this.concurrency = concurrency;
            this.minBatchSize = minBatchSize;
        }

        public int topK() {
            return topK;
        }

        public int topN() {
            return topN;
        }

        public double similarityCutoff() {
            return similarityCutoff;
        }

        public int degreeCutoff() {
            return degreeCutoff;
        }

        public int concurrency() {
            return concurrency;
        }

        public int minBatchSize() {
            return minBatchSize;
        }

        public boolean isParallel() {
            return concurrency > 1;
        }

        public boolean hasTopK() {
            return topK != 0;
        }

        public boolean hasTopN() {
            return topN != 0;
        }
    }

    private static final class VectorComputer implements RelationshipConsumer {

        long lastTarget = -1;
        LongArrayList targetIds;

        @Override
        public boolean accept(long source, long target) {
            if (source != target && lastTarget != target) {
                targetIds.add(target);
            }
            lastTarget = target;
            return true;
        }

        void reset(int degree) {
            lastTarget = -1;
            targetIds = new LongArrayList(degree, ARRAY_SIZING_STRATEGY);
        }
    }

    private static final class DegreeComputer implements RelationshipConsumer {

        long lastTarget = -1;
        int degree = 0;

        @Override
        public boolean accept(long source, long target) {
            if (source != target && lastTarget != target) {
                degree++;
            }
            lastTarget = target;
            return true;
        }

        void reset() {
            lastTarget = -1;
            degree = 0;
        }
    }
}
