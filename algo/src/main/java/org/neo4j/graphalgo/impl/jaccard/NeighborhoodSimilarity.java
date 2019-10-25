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
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class NeighborhoodSimilarity extends Algorithm<NeighborhoodSimilarity> {

    private final Graph graph;
    private final Config config;

    private final ExecutorService executorService;
    private final AllocationTracker tracker;
    private final Log log;

    private final BitSet nodeFilter;

    private HugeObjectArray<long[]> vectors;

    public NeighborhoodSimilarity(
            Graph graph,
            Config config,
            ExecutorService executorService,
            AllocationTracker tracker,
            Log log) {
        this.graph = graph;
        this.config = config;
        this.executorService = executorService;
        this.tracker = tracker;
        this.log = log;
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

    private static final ArraySizingStrategy ARRAY_SIZING_STRATEGY =
        (currentBufferLength, elementsCount, expectedAdditions) -> expectedAdditions + elementsCount;

    /**
     * Requires:
     * - Input graph must be bipartite:
     * (:Person)-[:LIKES]->(:Thing)
     * We collect all targets and use them only in the vectors, not as the thing for which we compute similarity.
     * If (:Person)-[:LIKES]->(:Person) we would filter out the person nodes.
     *
     * Number of results: (n^2 - n) / 2
     */
    public Stream<SimilarityResult> computeToStream(Direction direction) {

        this.vectors = HugeObjectArray.newArray(long[].class, graph.nodeCount(), tracker);

        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                "Direction BOTH is not supported by the NeighborhoodSimilarity algorithm.");
        }

        graph.forEachNode(node -> {
            if (graph.degree(node, direction) >= config.degreeCutoff) {
                nodeFilter.set(node);
            }
            return true;
        });

        graph.forEachNode(node -> {
            if (nodeFilter.get(node)) {
                int degree = graph.degree(node, direction);
                final LongArrayList targetIds = new LongArrayList(degree, ARRAY_SIZING_STRATEGY);
                graph.forEachRelationship(node, direction, (source, target) -> {
                    targetIds.add(target);
                    return true;
                });
                vectors.set(node, targetIds.buffer);
            }
            return true;
        });

        // Generate initial similarities
        Stream<SimilarityResult> stream = init();

        // Compute topK if necessary
        if (config.topk != 0) {
            stream = topK(stream);
        }

        // Compute topN if necessary
        if (config.top != 0) {
            stream = topN(stream);
        }

        return stream;
    }

    public Graph computeToGraph(Direction direction) {
        return similarityGraph(computeToStream(direction));
    }

    private Stream<SimilarityResult> init() {
        return LongStream.range(0, graph.nodeCount())
            .filter(nodeFilter::get)
            .boxed()
            .flatMap(n1 -> {
                long[] v1 = vectors.get(n1);
                return LongStream.range(n1 + 1, graph.nodeCount())
                    .filter(nodeFilter::get)
                    .mapToObj(n2 -> jaccard(n1, n2, v1, vectors.get(n2)))
                    .filter(similarityResult -> similarityResult.similarity >= config.similarityCutoff);
            });
    }

    private Stream<SimilarityResult> topK(Stream<SimilarityResult> inputStream) {
        Comparator<SimilarityResult> comparator = config.topk > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        TopKMap topKMap = new TopKMap(vectors.size(), Math.abs(config.topk), comparator, tracker);

        inputStream
            .flatMap(similarity -> Stream.of(similarity, similarity.reverse()))
            .forEach(topKMap);

        return topKMap.stream();
    }

    private Stream<SimilarityResult> topN(Stream<SimilarityResult> similarities) {
        Comparator<SimilarityResult> comparator = config.top > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        return similarities.sorted(comparator).limit(Math.abs(config.top));
    }

    private SimilarityResult jaccard(long n1, long n2, long[] v1, long[] v2) {
        long intersection = Intersections.intersection3(v1, v2);
        double union = v1.length + v2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / union;
        return new SimilarityResult(n1, n2, similarity);
    }

    private Graph similarityGraph(Stream<SimilarityResult> similarities) {
        SimilarityGraphBuilder builder = new SimilarityGraphBuilder(graph, tracker);
        return builder.build(similarities);
    }

    public static final class Config {
        public static final Config DEFAULT = new NeighborhoodSimilarity.Config(
                0.0,
                0,
                0,
                0,
                Pools.DEFAULT_CONCURRENCY,
                ParallelUtil.DEFAULT_BATCH_SIZE);

        private final double similarityCutoff;
        private final double degreeCutoff;

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
                int minBatchSize) {
            this.similarityCutoff = similarityCutoff;
            this.degreeCutoff = Math.max(1, degreeCutoff);
            this.top = top;
            this.topk = topk;
            this.concurrency = concurrency;
            this.minBatchSize = minBatchSize;
        }
    }

}
