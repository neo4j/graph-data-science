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
package org.neo4j.gds.traversal;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public final class RandomWalkCountingNodeVisits extends Algorithm<HugeAtomicLongArray> {

    private final Concurrency concurrency;
    private final ExecutorService executorService;
    private final Graph graph;
    private final long randomSeed;
    private final WalkParameters walkParameters;
    private final List<Long> sourceNodes;

    public static RandomWalkCountingNodeVisits create(
        Graph graph,
        Concurrency concurrency,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        Optional<Long> maybeRandomSeed,
        ProgressTracker progressTracker,
        ExecutorService executorService,
        TerminationFlag terminationFlag
    ) {
        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                concurrency,
                weight -> weight >= 0,
                "RandomWalk only supports non-negative weights.",
                executorService
            );
        }

        return new RandomWalkCountingNodeVisits(
            graph,
            concurrency,
            executorService,
            walkParameters,
            sourceNodes,
            maybeRandomSeed.orElseGet(() -> new Random().nextLong()),
            progressTracker,
            terminationFlag
        );
    }

    private RandomWalkCountingNodeVisits(
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        long randomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.graph = graph;
        this.walkParameters = walkParameters;
        this.sourceNodes = sourceNodes;
        this.randomSeed = randomSeed;
        this.terminationFlag = terminationFlag;

    }

    @Override
    public HugeAtomicLongArray compute() {
        progressTracker.beginSubTask("RandomWalk");

        var result = HugeAtomicLongArray.of(
            graph.nodeCount(),
            ParalleLongPageCreator.of(concurrency, l -> 0L)
        );

        var taskSupplier = createRandomWalkTaskSupplier(result);
        var tasks = IntStream
            .range(0, this.concurrency.value())
            .mapToObj(i -> taskSupplier.get())
            .toList();

        RunWithConcurrency.builder()
            .executor(this.executorService)
            .concurrency(this.concurrency)
            .tasks(tasks)
            .terminationFlag(this.terminationFlag)
            .mayInterruptIfRunning(true)
            .run();

        progressTracker.endSubTask("RandomWalk");

        return result;
    }


    private RandomWalkTaskSupplier createRandomWalkTaskSupplier(HugeAtomicLongArray result) {
        var nextNodeSupplier = RandomWalkCompanion.nextNodeSupplier(graph, sourceNodes);
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier = RandomWalkCompanion.cumulativeWeights(
            graph,
            concurrency,
            executorService,
            progressTracker
        );
        return new RandomWalkTaskSupplier(
            graph::concurrentCopy,
            nextNodeSupplier,
            cumulativeWeightSupplier,
            walkParameters,
            randomSeed,
            result,
            progressTracker,
            terminationFlag
        );
    }

    private static class RandomWalkTaskSupplier implements Supplier<RandomWalkTask> {
        private final Supplier<Graph> graphSupplier;
        private final NextNodeSupplier nextNodeSupplier;
        private final RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier;
        private final WalkParameters walkParameters;
        private final long randomSeed;
        private final HugeAtomicLongArray result;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;

        RandomWalkTaskSupplier(
            Supplier<Graph> graphSupplier,
            NextNodeSupplier nextNodeSupplier,
            RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
            WalkParameters walkParameters,
            long randomSeed, HugeAtomicLongArray result,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            this.graphSupplier = graphSupplier;
            this.nextNodeSupplier = nextNodeSupplier;
            this.cumulativeWeightSupplier = cumulativeWeightSupplier;
            this.walkParameters = walkParameters;
            this.randomSeed = randomSeed;
            this.result = result;
            this.progressTracker = progressTracker;
            this.terminationFlag = terminationFlag;
        }

        @Override
        public RandomWalkTask get() {
            var graph = graphSupplier.get();
            var sampler = RandomWalkSampler.create(
                graph,
                cumulativeWeightSupplier,
                walkParameters.walkLength(),
                walkParameters.returnFactor(),
                walkParameters.inOutFactor(),
                randomSeed
            );
            return new RandomWalkTask(
                graph,
                nextNodeSupplier,
                sampler,
                walkParameters.walksPerNode(),
                result,
                progressTracker,
                terminationFlag

            );
        }
    }

    private static final class RandomWalkTask implements Runnable {

        private final Graph graph;
        private final NextNodeSupplier nextNodeSupplier;
        private final RandomWalkSampler sampler;
        private final int walksPerNode;
        private final HugeAtomicLongArray result;
        private final ProgressTracker progressTracker;
        private final TerminationFlag terminationFlag;

        private RandomWalkTask(
            Graph graph, NextNodeSupplier nextNodeSupplier,
            RandomWalkSampler sampler, int walksPerNode, HugeAtomicLongArray result,
            ProgressTracker progressTracker,
            TerminationFlag terminationFlag
        ) {
            this.graph = graph;
            this.nextNodeSupplier = nextNodeSupplier;
            this.sampler = sampler;
            this.walksPerNode = walksPerNode;
            this.result = result;
            this.progressTracker = progressTracker;
            this.terminationFlag = terminationFlag;
        }

        public void run() {
            long nodeId;

            while (terminationFlag.running()) {
                nodeId = nextNodeSupplier.nextNode();

                if (nodeId == NextNodeSupplier.NO_MORE_NODES) break;

                if (graph.degree(nodeId) == 0) {
                    progressTracker.logProgress();
                    continue;
                }
                sampler.prepareForNewNode(nodeId);

                for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                    var path = sampler.walk(nodeId);
                    for (var n : path) {
                        result.getAndAdd(n, 1);
                    }
                }
                progressTracker.logProgress();
            }
        }

    }
}
