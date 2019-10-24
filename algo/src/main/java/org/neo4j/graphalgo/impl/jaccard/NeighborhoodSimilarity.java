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
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleTriangularMatrix;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

public class NeighborhoodSimilarity extends Algorithm<NeighborhoodSimilarity> {

    private final Graph graph;
    private final Config config;
    private final AllocationTracker tracker;
    private final Log log;

    public NeighborhoodSimilarity(
            Graph graph,
            Config config,
            ExecutorService executorService,
            AllocationTracker tracker,
            Log log) {
        this.graph = graph;
        this.config = config;
        this.tracker = tracker;
        this.log = log;
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
    public HugeDoubleTriangularMatrix run(Direction direction) {
        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                    "Direction BOTH is not supported by the NeighborhoodSimilarity algorithm.");
        }



        BitSet nodeFilter = new BitSet(graph.nodeCount());

        graph.forEachNode(node -> {
            if (graph.degree(node, direction) > 0L) {
                nodeFilter.set(node);
            }
            return true;
        });

        HugeObjectArray<long[]> vectors = HugeObjectArray.newArray(
                long[].class,
                graph.nodeCount(),
                AllocationTracker.EMPTY);

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

        HugeDoubleTriangularMatrix matrix = new HugeDoubleTriangularMatrix(graph.nodeCount(), tracker);
        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, graph.nodeCount())
                .filter(nodeFilter::get), (s) ->
                s.forEach(n1 -> {
                    LongStream.range(n1 + 1, graph.nodeCount())
                        .filter(nodeFilter::get)
                        .forEach(n2 -> {
                            long[] v1 = vectors.get(n1);
                            long[] v2 = vectors.get(n2);
                            // TODO: Assumes that the targets are sorted, need to check
                            long intersection = Intersections.intersection3(v1, v2);
                            double union = v1.length + v2.length - intersection;
                            matrix.set(n1, n2, union == 0 ? 0 : intersection / union);
                        });
                })
        );

        return matrix;
    }

    public static final class Config {
        public static final Config DEFAULT = new NeighborhoodSimilarity.Config(
                0.0,
                0,
                0,
                0,
                Pools.DEFAULT_CONCURRENCY,
                ParallelUtil.DEFAULT_BATCH_SIZE);

        double similarityCutoff;
        double degreeCutoff;

        private int top;
        private int topk;

        int concurrency;
        int minBatchSize;

        public Config(
                double similarityCutoff,
                int degreeCutoff,
                int top,
                int topk,
                int concurrency,
                int minBatchSize) {
            this.similarityCutoff = similarityCutoff;
            this.degreeCutoff = degreeCutoff;
            this.top = top;
            this.topk = topk;
            this.concurrency = concurrency;
            this.minBatchSize = minBatchSize;
        }
    }

}
