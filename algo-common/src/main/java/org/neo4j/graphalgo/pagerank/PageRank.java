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
package org.neo4j.graphalgo.pagerank;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.graphalgo.result.CentralityResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * Partition based parallel Page Rank based on
 * "An Efficient Partition-Based Parallel PageRank Algorithm" [1]
 * <p>
 * Each partition thread has its local array of only the nodes that it is responsible for,
 * not for all nodes. Combined, all partitions hold all page rank scores for every node once.
 * Instead of writing partition files and transferring them across the network
 * (as done in the paper since they were concerned with parallelising across multiple nodes),
 * we use integer arrays to write the results to.
 * The actual score is upscaled from a double to an integer by multiplying it with {@code 100_000}.
 * <p>To avoid contention by writing to a shared array, we partition the result array.
 * During execution, the scores arrays
 * are shaped like this:</p>
 * <pre>
 *     [ executing partition ] -&gt; [ calculated partition ] -&gt; [ local page rank scores ]
 * </pre>
 * <p>
 * Each single partition writes in a partitioned array, calculation the scores
 * for every receiving partition. A single partition only sees:
 * <pre>
 *     [ calculated partition ] -&gt; [ local page rank scores ]
 * </pre>
 * <p>The coordinating thread then builds the transpose of all written partitions from every partition:</p>
 * <pre>
 *     [ calculated partition ] -&gt; [ executing partition ] -&gt; [ local page rank scores ]
 * </pre>
 * <p>This step does not happen in parallel, but does not involve extensive copying.
 * The local page rank scores needn't be copied, only the partitioning arrays.
 * All in all, {@code concurrency^2} array element reads and assignments have to
 * be performed.</p>
 * <p>For the next iteration, every partition first updates its scores, in parallel.
 * A single partition now sees:</p>
 * <pre>
 *     [ executing partition ] -&gt; [ local page rank scores ]
 * </pre>
 * <p>That is, a list of all calculated scores for it self, grouped by the partition that
 * calculated these scores.
 * This means, most of the synchronization happens in parallel, too.</p>
 * <p>
 * Partitioning is not done by number of nodes but by the accumulated degree –
 * as described in "Fast Parallel PageRank: A Linear System Approach" [2].
 * Every partition should have about the same number of relationships to operate on.
 * This is done to avoid having one partition with super nodes and instead have
 * all partitions run in approximately equal time.
 * Smaller partitions are merged down until we have at most {@code concurrency} partitions,
 * in order to batch partitions and keep the number of threads in use predictable/configurable.
 * </p>
 * <p>
 * [1]: <a href="http://delab.csd.auth.gr/~dimitris/courses/ir_spring06/page_rank_computing/01531136.pdf">An Efficient Partition-Based Parallel PageRank Algorithm</a><br>
 * [2]: <a href="https://www.cs.purdue.edu/homes/dgleich/publications/gleich2004-parallel.pdf">Fast Parallel PageRank: A Linear System Approach</a>
 * </p>
 */
public class PageRank extends Algorithm<PageRank, PageRank> {

    public static final double DEFAULT_WEIGHT = 1.0D;
    public static final Double DEFAULT_TOLERANCE = 0.0000001D;

    private final ExecutorService executor;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final IdMapping idMapping;
    private final double dampingFactor;
    private final int maxIterations;
    private int ranIterations;
    private boolean didConverge;
    private final double toleranceValue;
    private final Graph graph;
    private final LongStream sourceNodeIds;
    private final PageRankVariant pageRankVariant;

    private ComputeSteps computeSteps;

    private final HugeDoubleArray result;

    /**
     * Parallel Page Rank implementation.
     * Whether the algorithm actually runs in parallel depends on the given
     * executor and batchSize.
     */
    PageRank(
        Graph graph,
        PageRankVariant pageRankVariant,
        LongStream sourceNodeIds,
        PageRankBaseConfig algoConfig,
        ExecutorService executor,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        assert algoConfig.maxIterations() >= 1;
        this.executor = executor;
        this.concurrency = algoConfig.concurrency();
        this.tracker = tracker;
        this.idMapping = graph;
        this.graph = graph;
        this.dampingFactor = algoConfig.dampingFactor();
        this.maxIterations = algoConfig.maxIterations();
        this.ranIterations = 0;
        this.didConverge = false;
        this.toleranceValue = algoConfig.tolerance();
        this.sourceNodeIds = sourceNodeIds;
        this.pageRankVariant = pageRankVariant;
        this.result = HugeDoubleArray.newArray(graph.nodeCount(), tracker);
        this.progressLogger = progressLogger;
    }

    public int iterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public double dampingFactor() {
        return dampingFactor;
    }

    /**
     * compute pageRank for n iterations
     */
    @Override
    public PageRank compute() {
        getProgressLogger().logMessage(":: Start");

        initializeSteps();
        computeSteps.run(maxIterations);
        computeSteps.mergeResults();

        getProgressLogger().logMessage(":: Finished");
        return this;
    }

    public CentralityResult result() {
        return new CentralityResult(result);
    }

    // we cannot do this in the constructor anymore since
    // we want to allow the user to provide a log instance
    private void initializeSteps() {
        if (computeSteps != null) {
            return;
        }

        var degreeBatchSize = BitUtil.ceilDiv(graph.relationshipCount(), concurrency);
        var partitions = PartitionUtils.degreePartition(graph, degreeBatchSize, Function.identity());

        ExecutorService executor = ParallelUtil.canRunInParallel(this.executor)
            ? this.executor
            : null;

        computeSteps = createComputeSteps(
            idMapping.nodeCount(),
            dampingFactor,
            sourceNodeIds.map(graph::toMappedNodeId).filter(mappedId -> mappedId != -1L).toArray(),
            partitions,
            executor
        );
    }

    private ComputeSteps createComputeSteps(
        long nodeCount,
        double dampingFactor,
        long[] sourceNodeIds,
        List<Partition> partitions,
        ExecutorService pool
    ) {

        List<ComputeStep> computeSteps = new ArrayList<>(partitions.size());
        LongArrayList starts = new LongArrayList(partitions.size());
        IntArrayList lengths = new IntArrayList(partitions.size());

        DegreeComputer degreeComputer = pageRankVariant.degreeComputer(graph);
        DegreeCache degreeCache = degreeComputer.degree(pool, concurrency, tracker);

        for (Partition partition : partitions) {
            int partitionSize = (int) partition.nodeCount();
            long start = partition.startNode();

            starts.add(start);
            lengths.add(partitionSize);

            computeSteps.add(pageRankVariant.createComputeStep(
                dampingFactor,
                toleranceValue,
                sourceNodeIds,
                graph,
                tracker,
                partitionSize,
                start,
                degreeCache,
                nodeCount,
                progressLogger
            ));
        }

        long[] startArray = starts.toArray();
        int[] lengthArray = lengths.toArray();
        for (ComputeStep computeStep : computeSteps) {
            computeStep.setStarts(startArray, lengthArray);
        }
        return new ComputeSteps(tracker, computeSteps, concurrency, pool);
    }

    @Override
    public PageRank me() {
        return this;
    }

    @Override
    public void release() {
        computeSteps.release();
    }

    public final class ComputeSteps {
        private List<ComputeStep> steps;
        private final ExecutorService pool;
        private float[][][] scores;
        private final int concurrency;

        private ComputeSteps(
            AllocationTracker tracker,
            List<ComputeStep> steps,
            int concurrency,
            ExecutorService pool
        ) {
            this.concurrency = concurrency;
            assert !steps.isEmpty();
            this.steps = steps;
            this.pool = pool;
            int stepSize = steps.size();
            scores = new float[stepSize][stepSize][];
            if (AllocationTracker.isTracking(tracker)) {
                tracker.add((stepSize + 1) * sizeOfObjectArray(stepSize));
            }
        }

        void mergeResults() {
            for (ComputeStep step : steps) {
                step.getPageRankResult(result);
            }
        }

        private void run(int iterations) {
            didConverge = false;
            ParallelUtil.runWithConcurrency(concurrency, steps, terminationFlag, pool);
            for (ranIterations = 0; ranIterations < iterations && !didConverge; ranIterations++) {
                getProgressLogger().logMessage(formatWithLocale(":: Iteration %d :: Start", ranIterations + 1));
                // calculate scores
                ParallelUtil.runWithConcurrency(concurrency, steps, terminationFlag, pool);

                // sync scores
                synchronizeScores();
                ParallelUtil.runWithConcurrency(concurrency, steps, terminationFlag, pool);
                didConverge = checkTolerance();

                // normalize deltas
                normalizeDeltas();
                ParallelUtil.runWithConcurrency(concurrency, steps, terminationFlag, pool);

                if ((ranIterations < iterations - 1) && !didConverge) {
                    getProgressLogger().reset(graph.relationshipCount());
                }

                getProgressLogger().logMessage(formatWithLocale(":: Iteration %d :: Finished", ranIterations + 1));
            }
        }

        private boolean checkTolerance() {
            return steps.stream().allMatch(ComputeStep::partitionIsStable);
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
                l2Norm += ParallelUtil.parallelStream(
                    Arrays.stream(deltas),
                    concurrency,
                    (stream) -> stream.map(score -> score * score).sum()
                );
            }
            l2Norm = Math.sqrt(l2Norm);
            l2Norm = l2Norm < 0 ? 1 : l2Norm;
            return l2Norm;
        }

        private void synchronizeScores() {
            int stepSize = steps.size();
            float[][][] scores = this.scores;
            int i;
            for (i = 0; i < stepSize; i++) {
                synchronizeScores(steps.get(i), i, scores);
            }
        }

        private void synchronizeScores(ComputeStep step, int idx, float[][][] scores) {
            step.prepareNextIteration(scores[idx]);
            float[][] nextScores = step.nextScores();
            for (int j = 0, len = nextScores.length; j < len; j++) {
                scores[j][idx] = nextScores[j];
            }
        }

        private void release() {
            if (AllocationTracker.isTracking(tracker)) {
                tracker.remove((scores.length + 1) * sizeOfObjectArray(scores.length));
            }
            steps.clear();
            steps = null;
            scores = null;
        }
    }
}
