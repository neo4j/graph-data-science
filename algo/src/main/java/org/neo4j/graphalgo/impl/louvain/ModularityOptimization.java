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
package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

import static org.neo4j.graphalgo.impl.louvain.Louvain.DEFAULT_WEIGHT;

/**
 * parallel weighted undirected modularity based community detection
 * (first phase of louvain algo). The algorithm assigns community ids to each
 * node in the graph. This is done by several threads in parallel. Each thread
 * performs a modularity optimization using a shuffled node iterator. The task
 * with the best (highest) modularity is selected and its community structure
 * is used as result
 *
 * @author mknblch
 */
public final class ModularityOptimization extends Algorithm<ModularityOptimization> {

    private static final MemoryEstimation MEMORY_ESTIMATION_TASK = MemoryEstimations
            .builder(Task.class)
            .perNode("sTot", HugeDoubleArray::memoryEstimation)
            .perNode("sIn", HugeDoubleArray::memoryEstimation)
            .perNode("localCommunities", HugeLongArray::memoryEstimation)
            .build();

    private static final MemoryEstimation MEMORY_ESTIMATION = MemoryEstimations
            .builder(ModularityOptimization.class)
            .perNode("communities", HugeLongArray::memoryEstimation)
            .perNode("ki", HugeDoubleArray::memoryEstimation)
            .perThread("tasks", MEMORY_ESTIMATION_TASK)
            .build();

    private static final double MINIMUM_MODULARITY = -1.0;
    /**
     * only outgoing directions are visited since the graph itself must be loaded using {@code .asUndirected(true) } !
     */
    private static final Direction DIRECTION = Direction.OUTGOING;
    private static final int NONE = -1;
    private final long nodeCount;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final NodeProperties nodeProperties;
    private Graph graph;
    private ExecutorService pool;
    private final NodeIterator nodeIterator;
    private double sumOfAllWeights, sumOfAllWeightsSquared;
    private HugeLongArray communities;
    private HugeDoubleArray summedAdjacencyWeights;
    private int iterations;
    private double q = MINIMUM_MODULARITY;
    private final AtomicInteger counter = new AtomicInteger(0);
    private boolean randomNeighborSelection = false;

    public static MemoryEstimation memoryEstimation() {
        return MEMORY_ESTIMATION;
    }

    ModularityOptimization(
            final Graph graph,
            final NodeProperties nodeProperties,
            final ExecutorService pool,
            final int concurrency,
            final AllocationTracker tracker) {
        this.graph = graph;
        this.nodeProperties = nodeProperties;
        nodeCount = graph.nodeCount();
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.nodeIterator = createNodeIterator(concurrency);

        summedAdjacencyWeights = HugeDoubleArray.newArray(nodeCount, tracker);
        communities = HugeLongArray.newArray(nodeCount, tracker);
    }

    ModularityOptimization withRandomNeighborSelection(final boolean randomNeighborSelection) {
        this.randomNeighborSelection = randomNeighborSelection;
        return this;
    }

    /**
     * create a nodeiterator based on concurrency setting.
     * Concurrency 1 (single threaded) results in an ordered
     * nodeIterator while higher concurrency settings create
     * shuffled iterators
     *
     * @param concurrency
     * @return
     */
    private NodeIterator createNodeIterator(final int concurrency) {

        if (concurrency > 1) {
            return new RandomNodeIterator(nodeCount);
        }

        return new NodeIterator() {
            @Override
            public void forEachNode(final LongPredicate consumer) {
                for (long i = 0L; i < nodeCount; i++) {
                    if (!consumer.test(i)) {
                        return;
                    }
                }
            }

            @Override
            public PrimitiveLongIterator nodeIterator() {
                return PrimitiveLongCollections.range(0L, nodeCount);
            }
        };
    }

    /**
     * get the task with the best community distribution
     * (highest modularity value) of an array of tasks
     *
     * @return best task
     */
    private static Task best(final Iterable<Task> tasks) {
        Task best = null; // may stay null if no task improves the current q
        double q = MINIMUM_MODULARITY;
        for (Task task : tasks) {
            if (!task.improvement) {
                continue;
            }
            final double modularity = task.getModularity();
            if (modularity > q) {
                q = modularity;
                best = task;
            }
        }
        return best;
    }

    /**
     * init ki (sum of weights of node) & m
     */
    private void init() {
        sumOfAllWeights = .0;
        for (int node = 0; node < nodeCount; node++) {
            // since we use an undirected graph 2m is counted here
            graph.forEachRelationship(node, DIRECTION, DEFAULT_WEIGHT, (s, t, w) -> {
                sumOfAllWeights += w;
                summedAdjacencyWeights.addTo(s, w / 2);
                summedAdjacencyWeights.addTo(t, w / 2);
                return true;
            });
        }
        sumOfAllWeightsSquared = Math.pow(sumOfAllWeights, 2.0);
        communities.setAll(i -> i);
    }

    /**
     * compute first phase louvain
     *
     * @param maxIterations
     * @return
     */
    public ModularityOptimization compute(final int maxIterations) {
        final TerminationFlag terminationFlag = getTerminationFlag();
        // init helper values & initial community structure
        init();
        // create an array of tasks for parallel exec
        final Collection<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        // as long as maxIterations is not reached
        for (iterations = 0; iterations < maxIterations; iterations++) {
            // reset node counter (for logging)
            counter.set(0);
            // run all tasks
            ParallelUtil.runWithConcurrency(concurrency, tasks, terminationFlag, pool);
            // take the best candidate
            Task candidate = best(tasks);
            if (null == candidate || candidate.q <= this.q) {
                // best candidate's modularity did not improve
                break;
            }
            // save current modularity
            this.q = candidate.q;
            // sync all tasks with the best candidate for the next round
            sync(candidate, tasks);
        }
        return this;
    }

    /**
     * sync parent Task with all other task except itself and
     * copy community structure to global community structure
     */
    private void sync(final Task parent, final Iterable<Task> tasks) {
        for (Task task : tasks) {
            task.improvement = false;
            if (task == parent) {
                continue;
            }
            task.sync(parent);
        }
        parent.localCommunities.copyTo(communities, nodeCount);
    }

    /**
     * get communities
     *
     * @return node-nodeId to localCommunities nodeId mapping
     */
    HugeLongArray getCommunityIds() {
        return communities;
    }

    /**
     * number of iterations
     *
     * @return number of iterations
     */
    public int getIterations() {
        return iterations;
    }

    double getModularity() {
        return q;
    }

    /**
     * @return this
     */
    @Override
    public ModularityOptimization me() {
        return this;
    }

    /**
     * release structures
     */
    @Override
    public void release() {
        tracker.remove(summedAdjacencyWeights.release());
        this.summedAdjacencyWeights = null;
        tracker.remove(communities.release());
        this.communities = null;
        this.graph = null;
        this.pool = null;
    }

    /**
     * Restartable task to perform modularity optimization
     */
    private final class Task implements Runnable {

        final HugeDoubleArray communityTotalWeights, communityInternalWeights;
        final HugeLongArray localCommunities;
        final RelationshipIterator rels;
        private final TerminationFlag terminationFlag;
        double q = MINIMUM_MODULARITY;
        boolean improvement = false;

        /**
         * at creation the task copies the community-structure
         * and initializes its helper arrays
         */
        Task() {
            terminationFlag = getTerminationFlag();
            communityTotalWeights = HugeDoubleArray.newArray(nodeCount, tracker);
            communityInternalWeights = HugeDoubleArray.newArray(nodeCount, tracker);
            localCommunities = HugeLongArray.newArray(nodeCount, tracker);
            rels = graph.concurrentCopy();
            summedAdjacencyWeights.copyTo(communityTotalWeights, nodeCount);
            communities.copyTo(localCommunities, nodeCount);
        }

        public MemoryEstimation memoryEstimation() {
            return MEMORY_ESTIMATION_TASK;
        }

        /**
         * copy community structure and helper arrays from parent
         * task into this task
         */
        void sync(final Task parent) {
            parent.localCommunities.copyTo(localCommunities, nodeCount);
            parent.communityTotalWeights.copyTo(communityTotalWeights, nodeCount);
            parent.communityInternalWeights.copyTo(communityInternalWeights, nodeCount);
            this.q = parent.q;
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
            final long denominator = nodeCount * concurrency;
            improvement = false;
            nodeIterator.forEachNode(node -> {
                final boolean move = move(node, localCommunities);
                improvement |= move;
                long count;
                if (((count = counter.incrementAndGet()) % TerminationFlag.RUN_CHECK_NODE_COUNT == 0)) {
                    progressLogger.logProgress(
                            count,
                            denominator,
                            () -> "round " + iterations);
                    terminationFlag.assertRunning();
                }
                return true;
            });
            this.q = calcModularity();
        }

        /**
         * get the graph modularity of the calculated community structure
         */
        double getModularity() {
            return q;
        }

        /**
         * calc modularity-gain for a node and move it into the best community
         *
         * @param node node nodeId
         * @return true if the node has been moved
         */
        private boolean move(
                final long node,
                final HugeLongArray localCommunities) {
            final long currentCommunity = localCommunities.get(node);

            double summedAdjacencyWeight = summedAdjacencyWeights.get(node);

            // remove the current nodes influence from its community
            communityTotalWeights.addTo(currentCommunity, -summedAdjacencyWeight);

            int degree = graph.degree(node, DIRECTION);
            LongDoubleMap communityWeights = new LongDoubleHashMap(degree);
            MutableDouble selfRelationshipWeight = new MutableDouble(0.0);
            rels.forEachRelationship(node, DIRECTION, DEFAULT_WEIGHT, (s, t, weight) -> {
                long targetCommunity = localCommunities.get(t);
                if (s != t) {
                    communityWeights.addTo(targetCommunity, weight);
                } else {
                    selfRelationshipWeight.add(weight);
                }
                return true;
            });

            final double initialWeight = communityWeights.get(currentCommunity) + selfRelationshipWeight.doubleValue();

            // remove current node from community internal weights
            communityInternalWeights.addTo(currentCommunity, -2.0 * (initialWeight + nodeProperties.nodeProperty(node)));

            localCommunities.set(node, NONE);
            double bestGain = .0;
            double bestWeight = initialWeight;
            long bestCommunity = currentCommunity;

            if (!communityWeights.isEmpty()) {
                if (randomNeighborSelection) {
                    bestCommunity = communityWeights.iterator().next().key;
                } else {
                    for (LongDoubleCursor cursor : communityWeights) {
                        long community = cursor.key;
                        double summedWeight = cursor.value;
                        final double gain = summedWeight / sumOfAllWeights - communityTotalWeights.get(community) * summedAdjacencyWeight / sumOfAllWeightsSquared;
                        if (gain > bestGain) {
                            bestGain = gain;
                            bestCommunity = community;
                            bestWeight = summedWeight;
                        }
                    }
                }
            }

            // add back the current nodes adjacent weights to its possibly new community
            communityTotalWeights.addTo(bestCommunity, summedAdjacencyWeight);

            // add weight of possibly better community for current node
            communityInternalWeights.addTo(bestCommunity, 2.0 * (bestWeight + nodeProperties.nodeProperty(node)));
            localCommunities.set(node, bestCommunity);
            return bestCommunity != currentCommunity;
        }

        private double calcModularity() {
            final Pointer.DoublePointer pointer = Pointer.wrap(.0);
            for (long node = 0L; node < nodeCount; node++) {
                rels.forEachRelationship(node, Direction.OUTGOING, DEFAULT_WEIGHT, (s, t, w) -> {
                    if (localCommunities.get(s) == localCommunities.get(t)) {
                        pointer.v += (w - (summedAdjacencyWeights.get(s) * summedAdjacencyWeights.get(t) / sumOfAllWeights));
                    }
                    return true;
                });
            }
            return pointer.v / sumOfAllWeights;
        }
    }
}
