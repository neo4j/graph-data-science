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
package org.neo4j.gds.graphsampling.samplers.rw.cnarw;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.SeenNodes;
import org.neo4j.gds.graphsampling.samplers.rw.InitialStartQualities;
import org.neo4j.gds.graphsampling.samplers.rw.WalkQualities;
import org.neo4j.gds.graphsampling.samplers.rw.Walker;
import org.neo4j.gds.graphsampling.samplers.rw.WalkerProducer;

import java.util.Optional;
import java.util.SplittableRandom;

import static org.neo4j.gds.graphsampling.samplers.rw.RandomWalkCompanion.initializeTotalWeights;

public class CommonNeighbourAwareRandomWalk implements RandomWalkBasedNodesSampler {
    private LongHashSet startNodesUsed;

    private static final double QUALITY_THRESHOLD_BASE = 0.05;
    
    private final CommonNeighbourAwareRandomWalkConfig config;
    private final WalkerProducer walkerProducer;

    public CommonNeighbourAwareRandomWalk(CommonNeighbourAwareRandomWalkConfig config) {

        this.config = config;
        this.walkerProducer = WalkerProducer.CNARWWalkerProducer();
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

        progressTracker.beginSubTask("Do common neighbour aware random walks");
        progressTracker.setSteps(seenNodes.totalExpectedNodes());

        startNodesUsed = new LongHashSet();
        var rng = new SplittableRandom(config.randomSeed().orElseGet(() -> new SplittableRandom().nextLong()));
        var initialStartQualities = InitialStartQualities.init(inputGraph, rng, config.startNodes());
        Optional<HugeAtomicDoubleArray> totalWeights = initializeTotalWeights(config, inputGraph.nodeCount());

        var tasks = ParallelUtil.tasks(config.concurrency(), () ->
            walkerProducer.getWalker(
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

        progressTracker.endSubTask("Do common neighbour aware random walks");

        progressTracker.endSubTask("Sample nodes");

        return seenNodes.sampledNodes();
    }

    public static MemoryEstimation memoryEstimation(RandomWalkWithRestartsConfig config) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(CommonNeighbourAwareRandomWalk.class)
            .perNode("seenNodes", HugeAtomicBitSet::memoryEstimation)
            // initialStartQualities has two 8-bytes-element array lists
            .fixed("initialStartQualities", 2L * 8L * config.startNodes().size());
        if (config.hasRelationshipWeightProperty()) {
            builder.perNode("totalWeights", HugeAtomicDoubleArray::memoryEstimation);
        }
        builder.perNode("random walks", nodeCount -> {
            // Three 8-bytes-element structures of size nodeCount * samplingRatio
            // Two 8-bytes-element structures of size nodeCount
            // Used for every thread
            return (long) (config.concurrency() * nodeCount * (config.samplingRatio() * 3L * 8L + 2L * 8L));
        });
        builder.perNode("startNodesUsed", nodeCount -> (long) (nodeCount * config.samplingRatio() * 8L));
        return builder.build();
    }

    public LongSet startNodesUsed() {
        return startNodesUsed;
    }

    @Override
    public long startNodesCount() {
        return startNodesUsed.size();
    }

    @Override
    public Task progressTask(GraphStore graphStore) {
        if (config.nodeLabelStratification()) {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf("Count node labels", graphStore.nodeCount()),
                Tasks.leaf(
                    "Do common neighbour aware random walks",
                    10 * Math.round(graphStore.nodeCount() * config.samplingRatio())
                )
            );
        } else {
            return Tasks.task(
                "Sample nodes",
                Tasks.leaf(
                    "Do common neighbour aware random walks",
                    10 * Math.round(graphStore.nodeCount() * config.samplingRatio())
                )
            );
        }
    }

    @Override
    public String progressTaskName() {
        return "Common neighbour aware random walks sampling";
    }


}
