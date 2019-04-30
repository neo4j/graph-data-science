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
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.NodeWeights;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.sources.RandomHugeNodeIterator;
import org.neo4j.graphalgo.core.sources.RandomNodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pointer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

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
public final class HugeModularityOptimization extends Algorithm<HugeModularityOptimization> {

    private static final double MINIMUM_MODULARITY = -1.0;
    /**
     * only outgoing directions are visited since the graph itself must be loaded using {@code .asUndirected(true) } !
     */
    private static final Direction D = Direction.OUTGOING;
    private static final int NONE = -1;
    private final long nodeCount;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final HugeNodeWeights nodeWeights;
    private Graph graph;
    private ExecutorService pool;
    private final NodeIterator nodeIterator;
    private double m2, m22;
    private HugeLongArray communities;
    private HugeDoubleArray ki;
    private int iterations;
    private double q = MINIMUM_MODULARITY;
    private final AtomicInteger counter = new AtomicInteger(0);
    private boolean randomNeighborSelection = false;
    private final Random random;

    HugeModularityOptimization(
            Graph graph,
            NodeWeights nodeWeights,
            ExecutorService pool,
            int concurrency,
            AllocationTracker tracker,
            long rndSeed) {
        this.graph = graph;
        this.nodeWeights = nodeWeights;
        nodeCount = graph.nodeCount();
        this.pool = pool;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.nodeIterator = createNodeIterator(concurrency);
        this.random = new Random(rndSeed);

        ki = HugeDoubleArray.newArray(nodeCount, tracker);
        communities = HugeLongArray.newArray(nodeCount, tracker);
    }

    public HugeModularityOptimization withRandomNeighborOptimization(boolean randomNeighborSelection) {
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
    private NodeIterator createNodeIterator(int concurrency) {

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
            public PrimitiveLongIterator hugeNodeIterator() {
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
    private static Task best(Collection<Task> tasks) {
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
        m2 = .0;
        for (int node = 0; node < nodeCount; node++) {
            // since we use an undirected graph 2m is counted here
            graph.forEachRelationship(node, D, (s, t) -> {
                final double w = graph.weightOf(s, t);
                m2 += w;
                ki.addTo(s, w / 2);
                ki.addTo(t, w / 2);
                return true;
            });
        }
        m22 = Math.pow(m2, 2.0);
        communities.setAll(i -> i);
    }

    /**
     * compute first phase louvain
     *
     * @param maxIterations
     * @return
     */
    public HugeModularityOptimization compute(int maxIterations) {
        final TerminationFlag terminationFlag = getTerminationFlag();
        // init helper values & initial community structure
        init();
        // create an array of tasks for parallel exec
        final ArrayList<Task> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            tasks.add(new Task());
        }
        // as long as maxIterations is not reached
        for (iterations = 0; iterations < maxIterations && terminationFlag.running(); iterations++) {
            // reset node counter (for logging)
            counter.set(0);
            // run all tasks
            ParallelUtil.runWithConcurrency(concurrency, tasks, pool);
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
    private void sync(Task parent, Collection<Task> tasks) {
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
    public HugeLongArray getCommunityIds() {
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

    public double getModularity() {
        return q;
    }

    /**
     * @return this
     */
    @Override
    public HugeModularityOptimization me() {
        return this;
    }

    /**
     * release structures
     *
     * @return this
     */
    @Override
    public HugeModularityOptimization release() {
        tracker.remove(ki.release());
        this.ki = null;
        tracker.remove(communities.release());
        this.communities = null;
        this.graph = null;
        this.pool = null;
        return this;
    }

    /**
     * Restartable task to perform modularity optimization
     */
    private final class Task implements Runnable {

        final HugeDoubleArray sTot, sIn;
        final HugeLongArray localCommunities;
        final RelationshipIterator rels;
        private final TerminationFlag terminationFlag;
        double bestGain, bestWeight, q = MINIMUM_MODULARITY;
        long bestCommunity;
        boolean improvement = false;

        /**
         * at creation the task copies the community-structure
         * and initializes its helper arrays
         */
        Task() {
            terminationFlag = getTerminationFlag();
            sTot = HugeDoubleArray.newArray(nodeCount, tracker);
            ki.copyTo(sTot, nodeCount);
            localCommunities = HugeLongArray.newArray(nodeCount, tracker);
            rels = graph.concurrentCopy();
            communities.copyTo(localCommunities, nodeCount);
            sIn = HugeDoubleArray.newArray(nodeCount, tracker);
            sIn.fill(0.);
        }

        /**
         * copy community structure and helper arrays from parent
         * task into this task
         */
        void sync(Task parent) {
            parent.localCommunities.copyTo(localCommunities, nodeCount);
            parent.sTot.copyTo(sTot, nodeCount);
            parent.sIn.copyTo(sIn, nodeCount);
            this.q = parent.q;
        }

        @Override
        public void run() {
            final ProgressLogger progressLogger = getProgressLogger();
            final long denominator = nodeCount * concurrency;
            improvement = false;
            nodeIterator.forEachNode(node -> {
                final boolean move = move(node);
                improvement |= move;
                progressLogger.logProgress(
                        counter.getAndIncrement(),
                        denominator,
                        () -> String.format("round %d", iterations + 1));
                return terminationFlag.running();
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
        private boolean move(long node) {
            final long currentCommunity = bestCommunity = localCommunities.get(node);

            int degree = graph.degree(node, D);
            HugeLongArray communitiesInOrder = HugeLongArray.newArray(degree, tracker);
            LongDoubleMap communityWeights = new LongDoubleHashMap(degree);

            final long[] communityCount = {0L};
            rels.forEachRelationship(node, D, (s, t) -> {
                double weight = graph.weightOf(s, t);
                long localCommunity = localCommunities.get(t);
                if (communityWeights.containsKey(localCommunity)) {
                    communityWeights.addTo(localCommunity, weight);
                } else {
                    communityWeights.put(localCommunity, weight);
                    communitiesInOrder.set(communityCount[0]++, localCommunity);
                }

                return true;
            });

            final double w = communityWeights.get(currentCommunity);
            sTot.addTo(currentCommunity, -ki.get(node));
            sIn.addTo(currentCommunity, -2.0 * (w + nodeWeights.nodeWeight(node)));

            removeWeightForSelfRelationships(node, communityWeights);

            localCommunities.set(node, NONE);
            bestGain = .0;
            bestWeight = w;

            if (degree > 0) {
                if (randomNeighborSelection) {
                    long index = (long) (random.nextDouble() * communitiesInOrder.size());
                    bestCommunity = communitiesInOrder.get(index);
                } else {
                    for (int i = 0; i < communityCount[0]; i++) {
                        long community = communitiesInOrder.get(i);
                        double wic = communityWeights.get(community);
                        final double g = wic / m2 - sTot.get(community) * ki.get(node) / m22;
                        if (g > bestGain) {
                            bestGain = g;
                            bestCommunity = community;
                            bestWeight = wic;
                        }
                    }
                }
            }

            sTot.addTo(bestCommunity, ki.get(node));
            sIn.addTo(bestCommunity, 2.0 * (bestWeight + nodeWeights.nodeWeight(node)));
            localCommunities.set(node, bestCommunity);
            return bestCommunity != currentCommunity;
        }

        private void removeWeightForSelfRelationships(long node, LongDoubleMap communityWeights) {
            rels.forEachRelationship(node, D, (s, t) -> {
                if (s == t) {
                    double currentWeight = communityWeights.get(localCommunities.get(s));
                    communityWeights.put(localCommunities.get(s), currentWeight - graph.weightOf(s, t));
                }
                return true;
            });
        }

        private double calcModularity() {
            final Pointer.DoublePointer pointer = Pointer.wrap(.0);
            for (long node = 0L; node < nodeCount; node++) {
                rels.forEachOutgoing(node, (s, t) -> {
                    if (localCommunities.get(s) != localCommunities.get(t)) {
                        return true;
                    }
                    pointer.map(v -> v + graph.weightOf(s, t) - (ki.get(s) * ki.get(t) / m2));
                    return true;
                });
            }
            return pointer.v / m2;
        }
    }
}
