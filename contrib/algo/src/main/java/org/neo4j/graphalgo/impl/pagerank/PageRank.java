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
package org.neo4j.graphalgo.impl.pagerank;

import com.carrotsearch.hppc.IntArrayList;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.results.DoubleArrayResult;
import org.neo4j.graphalgo.impl.results.PartitionedPrimitiveDoubleArrayResult;
import org.neo4j.graphdb.Direction;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;


/**
 * Partition based parallel PageRank based on
 * "An Efficient Partition-Based Parallel PageRank Algorithm" [1]
 * <p>
 * Each partition thread has its local array of only the nodes that it is responsible for,
 * not for all nodes. Combined, all partitions hold all page rank scores for every node once.
 * Instead of writing partition files and transferring them across the network
 * (as done in the paper since they were concerned with parallelising across multiple nodes),
 * we use integer arrays to write the results to.
 * The actual score is upscaled from a double to an integer by multiplying it with {@code 100_000}.
 * <p>
 * To avoid contention by writing to a shared array, we partition the result array.
 * During execution, the scores arrays
 * are shaped like this:
 * <pre>
 *     [ executing partition ] -> [ calculated partition ] -> [ local page rank scores ]
 * </pre>
 * Each single partition writes in a partitioned array, calculation the scores
 * for every receiving partition. A single partition only sees:
 * <pre>
 *     [ calculated partition ] -> [ local page rank scores ]
 * </pre>
 * The coordinating thread then builds the transpose of all written partitions from every partition:
 * <pre>
 *     [ calculated partition ] -> [ executing partition ] -> [ local page rank scores ]
 * </pre>
 * This step does not happen in parallel, but does not involve extensive copying.
 * The local page rank scores needn't be copied, only the partitioning arrays.
 * All in all, {@code concurrency^2} array element reads and assignments have to
 * be performed.
 * <p>
 * For the next iteration, every partition first updates its scores, in parallel.
 * A single partition now sees:
 * <pre>
 *     [ executing partition ] -> [ local page rank scores ]
 * </pre>
 * That is, a list of all calculated scores for it self, grouped by the partition that
 * calculated these scores.
 * This means, most of the synchronization happens in parallel, too.
 * <p>
 * Partitioning is not done by number of nodes but by the accumulated degree –
 * as described in "Fast Parallel PageRank: A Linear System Approach" [2].
 * Every partition should have about the same number of relationships to operate on.
 * This is done to avoid having one partition with super nodes and instead have
 * all partitions run in approximately equal time.
 * Smaller partitions are merged down until we have at most {@code concurrency} partitions,
 * in order to batch partitions and keep the number of threads in use predictable/configurable.
 * <p>
 * [1]: <a href="http://delab.csd.auth.gr/~dimitris/courses/ir_spring06/page_rank_computing/01531136.pdf">An Efficient Partition-Based Parallel PageRank Algorithm</a><br>
 * [2]: <a href="https://www.cs.purdue.edu/homes/dgleich/publications/gleich2004-parallel.pdf">Fast Parallel PageRank: A Linear System Approach</a>
 */
public class PageRank extends Algorithm<PageRank> implements PageRankAlgorithm {

    private final ComputeSteps computeSteps;

    /**
     * Forces sequential use. If you want parallelism, prefer
     * {@link #PageRank(ExecutorService, int, int, Graph, double, LongStream, PageRankVariant)}
     */
    PageRank(Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
             PageRankVariant pageRankVariant) {
        this(
                null,
                -1,
                ParallelUtil.DEFAULT_BATCH_SIZE,
                graph,
                dampingFactor,
                sourceNodeIds,
                pageRankVariant
                );
    }

    /**
     * Parallel Page Rank implementation.
     * Whether the algorithm actually runs in parallel depends on the given
     * executor and batchSize.
     */
    PageRank(
            ExecutorService executor,
            int concurrency,
            int batchSize,
            Graph graph,
            double dampingFactor,
            LongStream sourceNodeIds,
            PageRankVariant pageRankVariant) {
        List<Partition> partitions;
        if (ParallelUtil.canRunInParallel(executor)) {
            partitions = partitionGraph(
                    adjustBatchSize(batchSize),
                    graph,
                    graph,
                    graph);
        } else {
            executor = null;
            partitions = createSinglePartition(graph, graph);
        }

        DegreeComputer degreeComputer = pageRankVariant.degreeComputer(graph);

        computeSteps = createComputeSteps(
                concurrency,
                dampingFactor,
                sourceNodeIds.mapToInt(graph::toMappedNodeId).filter(mappedId -> mappedId != -1L).toArray(),
                graph,
                graph,
                graph,
                partitions,
                executor,
                pageRankVariant,
                degreeComputer,
                graph.nodeCount());
    }

    /**
     * compute pageRank for n iterations
     */
    @Override
    public PageRank compute(int iterations) {
        assert iterations >= 1;
        computeSteps.run(iterations);
        return this;
    }

    @Override
    public CentralityResult result() {
        return computeSteps.getPageRank();
    }

    @Override
    public Algorithm<?> algorithm() {
        return this;
    }

    private int adjustBatchSize(int batchSize) {
        // multiply batchsize by 8 as a very rough estimate of an average
        // degree of 8 for nodes, so that every partition has approx
        // batchSize nodes.
        batchSize <<= 3;
        return batchSize > 0 ? batchSize : Integer.MAX_VALUE;
    }

    private List<Partition> partitionGraph(
            int batchSize,
            IdMapping idMapping,
            NodeIterator nodeIterator,
            Degrees degrees) {
        int nodeCount = Math.toIntExact(idMapping.nodeCount());
        PrimitiveIntIterator nodes = nodeIterator.nodeIterator();
        List<Partition> partitions = new ArrayList<>();
        int start = 0;
        while (nodes.hasNext()) {
            Partition partition = new Partition(
                    nodeCount,
                    nodes,
                    degrees,
                    start,
                    batchSize);
            partitions.add(partition);
            start += partition.nodeCount;
        }
        return partitions;
    }

    private List<Partition> createSinglePartition(
            IdMapping idMapping,
            Degrees degrees) {
        return Collections.singletonList(
                new Partition(
                        Math.toIntExact(idMapping.nodeCount()),
                        null,
                        degrees,
                        0,
                        -1
                )
        );
    }

    private ComputeSteps createComputeSteps(
            int concurrency,
            double dampingFactor,
            int[] sourceNodeIds,
            RelationshipIterator relationshipIterator,
            WeightedRelationshipIterator weightedRelationshipIterator,
            Degrees degrees,
            List<Partition> partitions,
            ExecutorService pool,
            PageRankVariant pageRankVariant,
            DegreeComputer degreeComputer,
            long nodeCount) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_CONCURRENCY;
        }
        final int expectedParallelism = Math.min(
                concurrency,
                partitions.size());
        List<ComputeStep> computeSteps = new ArrayList<>(expectedParallelism);
        IntArrayList starts = new IntArrayList(expectedParallelism);
        IntArrayList lengths = new IntArrayList(expectedParallelism);
        int partitionsPerThread = ParallelUtil.threadSize(
                concurrency + 1,
                partitions.size());
        Iterator<Partition> parts = partitions.iterator();

        DegreeCache degreeCache = degreeComputer.degree(pool, concurrency);

        while (parts.hasNext()) {
            Partition partition = parts.next();
            int partitionCount = partition.nodeCount;
            int start = partition.startNode;
            for (int i = 1; i < partitionsPerThread && parts.hasNext(); i++) {
                partition = parts.next();
                partitionCount += partition.nodeCount;
            }

            starts.add(start);
            lengths.add(partitionCount);

            computeSteps.add(pageRankVariant.createComputeStep(
                    dampingFactor,
                    sourceNodeIds,
                    relationshipIterator,
                    weightedRelationshipIterator,
                    degrees,
                    partitionCount,
                    start,
                    degreeCache,
                    nodeCount
            ));
        }

        int[] startArray = starts.toArray();
        int[] lengthArray = lengths.toArray();
        for (ComputeStep computeStep : computeSteps) {
            computeStep.setStarts(startArray, lengthArray);
        }
        return new ComputeSteps(concurrency, computeSteps, pool);
    }

    @Override
    public PageRank me() {
        return this;
    }

    @Override
    public PageRank release() {
        computeSteps.release();
        return this;
    }

    private static final class Partition {

        private final int startNode;
        private final int nodeCount;

        Partition(
                int allNodeCount,
                PrimitiveIntIterator nodes,
                Degrees degrees,
                int startNode,
                int batchSize) {

            int nodeCount;
            int partitionSize = 0;
            if (batchSize > 0) {
                nodeCount = 0;
                while (partitionSize < batchSize && nodes.hasNext()) {
                    int nodeId = nodes.next();
                    ++nodeCount;
                    partitionSize += degrees.degree(nodeId, Direction.OUTGOING);
                }
            } else {
                nodeCount = allNodeCount;
            }

            this.startNode = startNode;
            this.nodeCount = nodeCount;
        }
    }

    private final class ComputeSteps {
        private final int concurrency;
        private List<ComputeStep> steps;
        private final ExecutorService pool;
        private float[][][] scores;

        private ComputeSteps(
                int concurrency,
                List<ComputeStep> steps,
                ExecutorService pool) {
            assert !steps.isEmpty();
            this.concurrency = concurrency;
            this.steps = steps;
            this.pool = pool;
            int stepSize = steps.size();
            scores = new float[stepSize][][];
            Arrays.setAll(scores, i -> new float[stepSize][]);
        }

        CentralityResult getPageRank() {
            ComputeStep firstStep = steps.get(0);
            if (steps.size() == 1) {
                return new DoubleArrayResult(firstStep.pageRank());
            }
            double[][] results = new double[steps.size()][];
            Iterator<ComputeStep> iterator = steps.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                results[i++] = iterator.next().pageRank();
            }
            return new PartitionedPrimitiveDoubleArrayResult(results, firstStep.starts());
        }

        private void run(int iterations) {
            // initialize data structures
            ParallelUtil.runWithConcurrency(concurrency, steps, pool);
            for (int iteration = 0; iteration < iterations && running(); iteration++) {
                // calculate scores
                ParallelUtil.runWithConcurrency(concurrency, steps, 3, 1, TimeUnit.SECONDS, pool);

                // sync scores
                synchronizeScores();
                ParallelUtil.runWithConcurrency(concurrency, steps, 3, 1, TimeUnit.SECONDS, pool);

                // normalize deltas
                normalizeDeltas();
                ParallelUtil.runWithConcurrency(concurrency, steps, 3, 1, TimeUnit.SECONDS, pool);
            }
        }

        private void normalizeDeltas() {
            double l2Norm = computeNorm();

            for (ComputeStep step : steps) {
                step.prepareNormalizeDeltas(l2Norm);
            }
        }

        private double computeNorm() {
            double l2Norm = 0.0;
            for (ComputeStep step : steps) {
                double[] deltas = step.deltas();
                l2Norm += Arrays.stream(deltas).parallel().map(score -> score * score).sum();
            }

            l2Norm = Math.sqrt(l2Norm);

            l2Norm = l2Norm <= 0 ? 1 : l2Norm;
            return l2Norm;
        }

        private void synchronizeScores() {
            int numberOfSteps = steps.size();
            float[][][] scores = this.scores;
            int stepIndex;
            for (stepIndex = 0; stepIndex < numberOfSteps; stepIndex++) {
                synchronizeScoresForStep(steps.get(stepIndex), stepIndex, scores);
            }
        }

        private void synchronizeScoresForStep(
                ComputeStep step,
                int idx,
                float[][][] scores) {
            step.prepareNextIteration(scores[idx]);
            float[][] nextScores = step.nextScores();
            for (int j = 0, len = nextScores.length; j < len; j++) {
                scores[j][idx] = nextScores[j];
            }
        }

        void release() {
            steps.clear();
            steps = null;
            scores = null;
        }
    }

}
