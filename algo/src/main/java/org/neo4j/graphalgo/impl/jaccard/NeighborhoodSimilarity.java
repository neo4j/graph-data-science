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
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class NeighborhoodSimilarity extends Algorithm<NeighborhoodSimilarity> {

    private final Graph graph;
    private final Config config;

    private final AllocationTracker tracker;

    private final BitSet nodeFilter;

    private HugeObjectArray<long[]> vectors;
    private long nodesToCompare;

    public NeighborhoodSimilarity(
        Graph graph,
        Config config,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
        this.nodeFilter = new BitSet(graph.nodeCount());
    }

    @Override
    public NeighborhoodSimilarity me() {
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

        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                "Direction BOTH is not supported by the NeighborhoodSimilarity algorithm.");
        }

        this.vectors = HugeObjectArray.newArray(long[].class, graph.nodeCount(), tracker);

        // Create a filter for which nodes to compare and calculate the neighborhood for each node
        prepare(direction);

        // Compute similarities
        Stream<SimilarityResult> stream = config.isParallel()
            ? computeParallel()
            : compute();

        // Log progress
        stream = log(stream);

        // Compute topN iff we did not run topK before.
        // Can not happen when algo is called from proc.
        if (config.hasTop() && !config.hasTopK()) {
            stream = computeTopN(stream);
        }
        return stream;
    }

    private Stream<SimilarityResult> compute() {
        return (config.hasTopK() && config.hasTop())
            ? computeTopN(computeTopkMap())
            : (config.hasTopK())
                ? computeTopkMap().stream()
                : computeAll();
    }

    private Stream<SimilarityResult> computeParallel() {
        return (config.hasTopK() && config.hasTop())
            ? computeTopN(computeTopKMapParallel())
            : (config.hasTopK())
                ? computeTopKMapParallel().stream()
                : computeAllParallel();
    }

    public SimilarityGraphResult computeToGraph(Direction direction) {
        Graph simGraph = similarityGraph(computeToStream(direction));
        return new SimilarityGraphResult(simGraph, nodesToCompare);
    }

    private void prepare(Direction direction) {
        vectors.setAll(node -> {
            int degree = graph.degree(node, direction);

            if (degree >= config.degreeCutoff) {
                nodesToCompare++;
                nodeFilter.set(node);

                final LongArrayList targetIds = new LongArrayList(degree, ARRAY_SIZING_STRATEGY);
                graph.forEachRelationship(node, direction, (source, target) -> {
                    targetIds.add(target);
                    return true;
                });
                return targetIds.buffer;
            }
            return null;
        });
    }

    private Stream<SimilarityResult> computeAll() {
        return new SetBitsIterable(nodeFilter).stream()
            .boxed()
            .flatMap(node1 -> {
                long[] vector1 = vectors.get(node1);
                return new SetBitsIterable(nodeFilter, node1 + 1).stream()
                    .mapToObj(node2 -> jaccard(node1, node2, vector1, vectors.get(node2)))
                    .filter(Objects::nonNull);
            });
    }

    private Stream<SimilarityResult> computeAllParallel() {
        return ParallelUtil.parallelStream(
            new SetBitsIterable(nodeFilter).stream(), stream -> stream
                .boxed()
                .flatMap(node1 -> {
                    long[] vector1 = vectors.get(node1);
                    return new SetBitsIterable(nodeFilter, node1 + 1).stream()
                        .mapToObj(node2 -> jaccard(node1, node2, vector1, vectors.get(node2)))
                        .filter(Objects::nonNull);
                })
        );
    }

    private TopKMap computeTopkMap() {
        Comparator<SimilarityResult> comparator = config.topk > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), nodeFilter, Math.abs(config.topk), comparator, tracker);
        new SetBitsIterable(nodeFilter).stream()
            .forEach(node1 -> {
                long[] vector1 = vectors.get(node1);
                new SetBitsIterable(nodeFilter, node1 + 1).stream()
                    .forEach(node2 -> {
                        double similarity = jaccardPrimitive(vector1, vectors.get(node2));
                        if (!Double.isNaN(similarity)) {
                            topKMap.put(node1, node2, similarity);
                            topKMap.put(node2, node1, similarity);
                        }
                    });
            });
        return topKMap;
    }

    private TopKMap computeTopKMapParallel() {
        Comparator<SimilarityResult> comparator = config.topk > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), nodeFilter, Math.abs(config.topk), comparator, tracker);
        ParallelUtil.parallelStreamConsume(
            new SetBitsIterable(nodeFilter).stream(),
            stream -> stream
                .forEach(node1 -> {
                    long[] vector1 = vectors.get(node1);
                    // We deliberately compute the full matrix (except the diagonal).
                    // The parallel workload is partitioned based on the outer stream.
                    // The TopKMap stores a priority queue for each node. Writing
                    // into these queues is not considered to be thread-safe.
                    // Hence, we need to ensure that down the stream, exactly one queue
                    // within the TopKMap processes all pairs for a single node.
                    new SetBitsIterable(nodeFilter).stream()
                        .filter(node2 -> node1 != node2)
                        .forEach(node2 -> {
                            double similarity = jaccardPrimitive(vector1, vectors.get(node2));
                            if (!Double.isNaN(similarity)) {
                                topKMap.put(node1, node2, similarity);
                            }
                        });
                })
        );
        return topKMap;
    }

    private Stream<SimilarityResult> log(Stream<SimilarityResult> stream) {
        long logInterval = Math.max(1, BitUtil.nearbyPowerOfTwo(nodesToCompare / 100));
        return stream.peek(sim -> {
            if ((sim.node1 & (logInterval - 1)) == 0) {
                progressLogger.logProgress(sim.node1, nodesToCompare);
            }
        });
    }

    private Stream<SimilarityResult> computeTopN(Stream<SimilarityResult> similarities) {
        Comparator<SimilarityResult> comparator = config.top > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        return similarities.sorted(comparator).limit(Math.abs(config.top));
    }

    private Stream<SimilarityResult> computeTopN(TopKMap topKMap) {
        int absTop = Math.abs(config.top);

        BoundedLongLongPriorityQueue topNLongPriorityQueue = config.top > 0
            ? BoundedLongLongPriorityQueue.max(absTop)
            : BoundedLongLongPriorityQueue.min(absTop);
        topKMap.forEach(topNLongPriorityQueue::offer);

        List<SimilarityResult> topNResults = new ArrayList<>(absTop);
        topNLongPriorityQueue.foreach((element1, element2, priority) ->
            topNResults.add(new SimilarityResult(element1, element2, priority)));
        return topNResults.stream();
    }

    private SimilarityResult jaccard(long node1, long node2, long[] vector1, long[] vector2) {
        long intersection = Intersections.intersection3(vector1, vector2);
        double union = vector1.length + vector2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / union;
        return similarity >= config.similarityCutoff ? new SimilarityResult(node1, node2, similarity) : null;
    }

    private double jaccardPrimitive(long[] vector1, long[] vector2) {
        long intersection = Intersections.intersection3(vector1, vector2);
        double union = vector1.length + vector2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / union;
        return similarity >= config.similarityCutoff ? similarity : Double.NaN;
    }

    private Graph similarityGraph(Stream<SimilarityResult> similarities) {
        SimilarityGraphBuilder builder = new SimilarityGraphBuilder(graph, tracker);
        return builder.build(similarities);
    }

    public static final class Config {

        private final double similarityCutoff;
        private final int degreeCutoff;

        private final int top;
        private final int topk;

        private final int concurrency;
        private final int minBatchSize;

        public Config(
            double similarityCutoff,
            int degreeCutoff,
            int top,
            int topk,
            int concurrency,
            int minBatchSize
        ) {
            this.similarityCutoff = similarityCutoff;
            // TODO: make this constraint more prominent
            this.degreeCutoff = Math.max(1, degreeCutoff);
            this.top = top;
            this.topk = topk;
            this.concurrency = concurrency;
            this.minBatchSize = minBatchSize;
        }

        public int topk() {
            return topk;
        }

        public int top() {
            return top;
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
            return topk != 0;
        }

        public boolean hasTop() {
            return top != 0;
        }
    }

}
