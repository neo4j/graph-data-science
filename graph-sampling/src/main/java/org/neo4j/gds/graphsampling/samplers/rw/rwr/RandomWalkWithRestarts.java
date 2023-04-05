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
package org.neo4j.gds.graphsampling.samplers.rw.rwr;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.graphsampling.NodesSampler;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rw.InitialStartQualities;
import org.neo4j.gds.graphsampling.samplers.rw.WalkQualities;
import org.neo4j.gds.graphsampling.samplers.rw.Walker;

import java.util.Optional;
import java.util.SplittableRandom;

public class RandomWalkWithRestarts implements NodesSampler {
    public static final double QUALITY_MOMENTUM = 0.9;
    private static final double QUALITY_THRESHOLD_BASE = 0.05;
    public static final int MAX_WALKS_PER_START = 100;
    public static final double TOTAL_WEIGHT_MISSING = -1.0;
    protected static final long INVALID_NODE_ID = -1;

    private final RandomWalkWithRestartsConfig config;
    private LongHashSet startNodesUsed;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config) {
        this.config = config;
    }

    @Override
    public HugeAtomicBitSet compute(Graph inputGraph, ProgressTracker progressTracker) {
        assert inputGraph.hasRelationshipProperty() == config.hasRelationshipWeightProperty();

        progressTracker.beginSubTask("Sample nodes");

        var seenNodes = SeenNodes.create(
            inputGraph,
            progressTracker,
            config.nodeLabelStratification(),
            config.concurrency(),
            config.samplingRatio()
        );

        progressTracker.beginSubTask("Do random walks");
        progressTracker.setSteps(seenNodes.totalExpectedNodes());

        startNodesUsed = new LongHashSet();
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));
        var initialStartQualities = InitialStartQualities.init(inputGraph, rng, config.startNodes());
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

        progressTracker.endSubTask("Do random walks");

        progressTracker.endSubTask("Sample nodes");

        return seenNodes.sampledNodes();
    }

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

    private Optional<HugeAtomicDoubleArray> initializeTotalWeights(long nodeCount) {
        if (config.hasRelationshipWeightProperty()) {
            var totalWeights = HugeAtomicDoubleArray.of(nodeCount, ParallelDoublePageCreator.passThrough(config.concurrency()));
            totalWeights.setAll(TOTAL_WEIGHT_MISSING);
            return Optional.of(totalWeights);
        }
        return Optional.empty();
    }

    public LongSet startNodesUsed() {
        return startNodesUsed;
    }

    protected Runnable getWalker(
        SeenNodes seenNodes,
        Optional<HugeAtomicDoubleArray> totalWeights,
        double qualityThreshold,
        WalkQualities walkQualities,
        SplittableRandom split,
        Graph concurrentCopy,
        RandomWalkWithRestartsConfig config,
        ProgressTracker progressTracker
    ) {
        return new Walker(seenNodes, totalWeights, qualityThreshold, walkQualities, split, concurrentCopy, config, progressTracker,
            new UniformNextNodeStrategy(split, concurrentCopy, totalWeights)
        );
    }
}
