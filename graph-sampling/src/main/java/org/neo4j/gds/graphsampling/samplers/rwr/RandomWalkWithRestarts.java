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
package org.neo4j.gds.graphsampling.samplers.rwr;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.DoubleCollection;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongCollection;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.graphsampling.NodesSampler;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.NodeLabelHistogram;

import java.util.Optional;
import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {
    protected static final double QUALITY_MOMENTUM = 0.9;
    protected static final double QUALITY_THRESHOLD_BASE = 0.05;
    protected static final int MAX_WALKS_PER_START = 100;
    protected static final double TOTAL_WEIGHT_MISSING = -1.0;
    protected static final long INVALID_NODE_ID = -1;

    private final RandomWalkWithRestartsConfig config;
    protected LongHashSet startNodesUsed;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config) {
        this.config = config;
    }

    @Override
    public HugeAtomicBitSet compute(Graph inputGraph, ProgressTracker progressTracker) {
        assert inputGraph.hasRelationshipProperty() == config.hasRelationshipWeightProperty();

        progressTracker.beginSubTask("Sample nodes");

        var seenNodes = getSeenNodes(inputGraph, progressTracker);

        progressTracker.beginSubTask(getSubTaskMessage());
        progressTracker.setSteps(seenNodes.totalExpectedNodes());

        startNodesUsed = new LongHashSet();
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));
        var initialStartQualities = initializeQualities(inputGraph, rng);
        Optional<HugeAtomicDoubleArray> totalWeights = initializeTotalWeights(inputGraph.nodeCount());

        var tasks = ParallelUtil.tasks(config.concurrency(), () ->
            getWalker(
                seenNodes,
                totalWeights,
                QUALITY_THRESHOLD_BASE / (config.concurrency() * config.concurrency()),
                new WalkQualities(initialStartQualities),
                rng.split(),
                inputGraph.concurrentCopy(),
                config,
                progressTracker
            )
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();
        tasks.forEach(task -> startNodesUsed.addAll(((Walker) task).startNodesUsed()));

        progressTracker.endSubTask(getSubTaskMessage());

        progressTracker.endSubTask("Sample nodes");

        return seenNodes.sampledNodes();
    }

    protected Runnable getWalker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double v,
        WalkQualities walkQualities,
        SplittableRandom split,
        Graph concurrentCopy,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker
    ) {
        return new Walker(seenNodes, totalWeights, v, walkQualities, split, concurrentCopy, config, progressTracker);
    }

    protected String getSubTaskMessage() {return "Do random walks";}

    @Override
    public Task progressTask(GraphStore graphStore) {
        if (config.nodeLabelStratification()) {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf("Count node labels", graphStore.nodeCount()),
                Tasks.leaf("Do random walks", 10 * Math.round(graphStore.nodeCount() * config.samplingRatio()))
            );
        } else {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf("Do random walks", 10 * Math.round(graphStore.nodeCount() * config.samplingRatio()))
            );
        }
    }

    @Override
    public String progressTaskName() {
        return "Random walk with restarts sampling";
    }

    private SeenNodes getSeenNodes(Graph inputGraph, ProgressTracker progressTracker) {
        if (config.nodeLabelStratification()) {
            var nodeLabelHistogram = NodeLabelHistogram.compute(
                inputGraph,
                config.concurrency(),
                progressTracker
            );

            return new SeenNodes.SeenNodesByLabelSet(inputGraph, nodeLabelHistogram, config.samplingRatio());
        }

        return new SeenNodes.GlobalSeenNodes(
            HugeAtomicBitSet.create(inputGraph.nodeCount()),
            Math.round(inputGraph.nodeCount() * config.samplingRatio())
        );
    }

    private Optional<HugeAtomicDoubleArray> initializeTotalWeights(long nodeCount) {
        if (config.hasRelationshipWeightProperty()) {
            var totalWeights = HugeAtomicDoubleArray.newArray(nodeCount);
            totalWeights.setAll(TOTAL_WEIGHT_MISSING);
            return Optional.of(totalWeights);
        }
        return Optional.empty();
    }

    @ValueClass
    interface InitialStartQualities {
        LongCollection nodeIds();

        DoubleCollection qualities();
    }

    private InitialStartQualities initializeQualities(Graph inputGraph, SplittableRandom rng) {
        var nodeIds = new LongArrayList();
        var qualities = new DoubleArrayList();

        if (!config.startNodes().isEmpty()) {
            config.startNodes().forEach(nodeId -> {
                nodeIds.add(inputGraph.toMappedNodeId(nodeId));
                qualities.add(1.0);
            });
        } else {
            nodeIds.add(rng.nextLong(inputGraph.nodeCount()));
            qualities.add(1.0);
        }

        return ImmutableInitialStartQualities.of(nodeIds, qualities);
    }

    public LongSet startNodesUsed() {
        return startNodesUsed;
    }

    protected static class Walker implements Runnable {

        protected final SeenNodes seenNodes;
        protected final Optional<HugeAtomicDoubleArray> totalWeights;
        protected final double qualityThreshold;
        protected final WalkQualities walkQualities;
        protected final SplittableRandom rng;
        protected final Graph inputGraph;
        protected final RandomWalkWithRestartsConfig config;
        protected final ProgressTracker progressTracker;

        protected final LongSet startNodesUsed;

        protected Walker(
            SeenNodes seenNodes,
            Optional<HugeAtomicDoubleArray> totalWeights,
            double qualityThreshold,
            WalkQualities walkQualities,
            SplittableRandom rng,
            Graph inputGraph,
            RandomWalkWithRestartsConfig config,
            ProgressTracker progressTracker
        ) {
            this.seenNodes = seenNodes;
            this.totalWeights = totalWeights;
            this.qualityThreshold = qualityThreshold;
            this.walkQualities = walkQualities;
            this.rng = rng;
            this.inputGraph = inputGraph;
            this.config = config;
            this.progressTracker = progressTracker;
            this.startNodesUsed = new LongHashSet();
        }

        @Override
        public void run() {
            int currentStartNodePosition = rng.nextInt(walkQualities.size());
            long currentNode = walkQualities.nodeId(currentStartNodePosition);
            startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
            int addedNodes = 0;
            int nodesConsidered = 1;
            int walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);

            while (!seenNodes.hasSeenEnough()) {
                if (seenNodes.addNode(currentNode)) {
                    addedNodes++;
                }

                // walk a step
                double degree = computeDegree(currentNode);
                if (degree == 0.0 || rng.nextDouble() < config.restartProbability()) {
                    progressTracker.logSteps(addedNodes);

                    double walkQuality = ((double) addedNodes) / nodesConsidered;
                    walkQualities.updateNodeQuality(currentStartNodePosition, walkQuality);
                    addedNodes = 0;
                    nodesConsidered = 1;

                    if (walksLeft-- > 0 && walkQualities.nodeQuality(currentStartNodePosition) > qualityThreshold) {
                        currentNode = walkQualities.nodeId(currentStartNodePosition);
                        continue;
                    }

                    if (walkQualities.nodeQuality(currentStartNodePosition) < 1.0 / MAX_WALKS_PER_START) {
                        walkQualities.removeNode(currentStartNodePosition);
                    }

                    if (walkQualities.expectedQuality() < qualityThreshold) {
                        long newNode;
                        do {
                            newNode = rng.nextLong(inputGraph.nodeCount());
                        } while (!walkQualities.addNode(newNode));
                    }

                    currentStartNodePosition = rng.nextInt(walkQualities.size());
                    currentNode = walkQualities.nodeId(currentStartNodePosition);
                    startNodesUsed.add(inputGraph.toOriginalNodeId(currentNode));
                    walksLeft = (int) Math.round(walkQualities.nodeQuality(currentStartNodePosition) * MAX_WALKS_PER_START);
                } else {
                    if (totalWeights.isPresent()) {
                        currentNode = weightedNextNode(currentNode);
                    } else {
                        int targetOffset = rng.nextInt(inputGraph.degree(currentNode));
                        currentNode = inputGraph.nthTarget(currentNode, targetOffset);
                        assert currentNode != IdMap.NOT_FOUND : "The offset '" + targetOffset + "' is bound by the degree but no target could be found for nodeId " + currentNode;
                    }
                    nodesConsidered++;
                }
            }
        }

        protected double computeDegree(long currentNode) {
            if (totalWeights.isEmpty()) {
                return inputGraph.degree(currentNode);
            }

            var presentTotalWeights = totalWeights.get();
            if (presentTotalWeights.get(currentNode) == TOTAL_WEIGHT_MISSING) {
                var degree = new MutableDouble(0.0);
                inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                    degree.add(weight);
                    return true;
                });
                presentTotalWeights.set(currentNode, degree.doubleValue());
            }

            return presentTotalWeights.get(currentNode);
        }

        protected long weightedNextNode(long currentNode) {
            var remainingMass = new MutableDouble(rng.nextDouble(0, computeDegree(currentNode)));
            var target = new MutableLong(INVALID_NODE_ID);

            inputGraph.forEachRelationship(currentNode, 0.0, (src, trg, weight) -> {
                if (remainingMass.doubleValue() < weight) {
                    target.setValue(trg);
                    return false;
                }
                remainingMass.subtract(weight);
                return true;
            });

            assert target.getValue() != -1;

            return target.getValue();
        }

        public LongSet startNodesUsed() {
            return startNodesUsed;
        }
    }

    /**
     * In order be able to sample start nodes uniformly at random (for performance reasons) we have a special data
     * structure which is optimized for exactly this. In particular, we need to be able to do random access by index
     * of the set of start nodes we are currently interested in. A simple hashmap for example does not work for this
     * reason.
     */
    protected static class WalkQualities {
        private final LongSet nodeIdIndex;
        private final LongArrayList nodeIds;
        private final DoubleArrayList qualities;
        private int size;
        private double sum;
        private double sumOfSquares;

        WalkQualities(InitialStartQualities initialStartQualities) {
            this.nodeIdIndex = new LongHashSet(initialStartQualities.nodeIds());
            this.nodeIds = new LongArrayList(initialStartQualities.nodeIds());
            this.qualities = new DoubleArrayList(initialStartQualities.qualities());
            this.sum = qualities.size();
            this.sumOfSquares = qualities.size();
            this.size = qualities.size();
        }

        public boolean addNode(long nodeId) {
            if (nodeIdIndex.contains(nodeId)) {
                return false;
            }

            if (size >= nodeIds.size()) {
                nodeIds.add(nodeId);
                qualities.add(1.0);
            } else {
                nodeIds.set(size, nodeId);
                qualities.set(size, 1.0);
            }
            nodeIdIndex.add(nodeId);
            size++;

            sum += 1.0;
            sumOfSquares += 1.0;

            return true;
        }

        public void removeNode(int position) {
            double quality = qualities.get(position);
            sum -= quality;
            sumOfSquares -= quality * quality;

            nodeIds.set(position, nodeIds.get(size - 1));
            qualities.set(position, qualities.get(size - 1));
            size--;
        }

        public long nodeId(int position) {
            return nodeIds.get(position);
        }

        public double nodeQuality(int position) {
            return qualities.get(position);
        }

        public void updateNodeQuality(int position, double walkQuality) {
            double previousQuality = qualities.get(position);
            double updatedQuality = QUALITY_MOMENTUM * previousQuality + (1 - QUALITY_MOMENTUM) * walkQuality;

            sum += updatedQuality - previousQuality;
            sumOfSquares += updatedQuality * updatedQuality - previousQuality * previousQuality;

            qualities.set(position, updatedQuality);
        }

        public double expectedQuality() {
            if (size <= 0) {
                return 0;
            }
            return sumOfSquares / sum;
        }

        public int size() {
            return size;
        }
    }
}
