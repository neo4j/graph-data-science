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
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class RandomWalk extends Algorithm<Stream<long[]>> {

    private final Graph graph;
    private final int concurrency;
    private final Optional<Long> maybeRandomSeed;
    private final List<Long> sourceNodes;
    private final int walkBufferSize;
    private final int walksPerNode;
    private final int walkLength;
    private final double inOutFactor;
    private final double returnFactor;
    private final ExecutorService executorService;

    private RandomWalk(
        Graph graph,
        int concurrency,
        Optional<Long> maybeRandomSeed,
        List<Long> sourceNodes,
        int walkBufferSize,
        int walksPerNode,
        int walkLength,
        double returnFactor,
        double inOutFactor,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.maybeRandomSeed = maybeRandomSeed;
        this.sourceNodes = sourceNodes;
        this.walkBufferSize = walkBufferSize;
        this.walksPerNode = walksPerNode;
        this.walkLength = walkLength;
        this.returnFactor = returnFactor;
        this.inOutFactor = inOutFactor;
        this.executorService = executorService;
    }

    public static RandomWalk create(
        Graph graph,
        RandomWalkBaseConfig config,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                config.concurrency(),
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                executorService
            );
        }

        return new RandomWalk(
            graph,
            config.concurrency(),
            config.randomSeed(),
            config.sourceNodes(),
            config.walkBufferSize(),
            config.walksPerNode(),
            config.walkLength(),
            config.returnFactor(),
            config.inOutFactor(),
            progressTracker,
            executorService
        );
    }

    @Override
    public Stream<long[]> compute() {
        progressTracker.beginSubTask("RandomWalk");

        var randomSeed = maybeRandomSeed.orElseGet(() -> new Random().nextLong());

        var cumulativeWeightSupplier = RandomWalkCompanion.cumulativeWeights(
            graph,
            concurrency,
            executorService,
            progressTracker
        );

        var nextNodeSupplier = RandomWalkCompanion.nextNodeSupplier(graph, sourceNodes);

        var terminationFlag = new ExternalTerminationFlag(this);

        BlockingQueue<long[]> walks = new ArrayBlockingQueue<>(walkBufferSize);
        long[] TOMB = new long[0];

        startWalkers(terminationFlag, cumulativeWeightSupplier, randomSeed, nextNodeSupplier, walks, TOMB);
        return walksQueueConsumer(terminationFlag, TOMB, walks);
    }


    private void startWalkers(
        TerminationFlag terminationFlag,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        long randomSeed,
        NextNodeSupplier nextNodeSupplier,
        BlockingQueue<long[]> walks,
        long[] TOMB
    ) {
        var tasks = IntStream
            .range(0, this.concurrency)
            .mapToObj(i ->
                new RandomWalkTask(
                    graph.concurrentCopy(),
                    nextNodeSupplier,
                    cumulativeWeightSupplier,
                    walks,
                    walksPerNode,
                    walkLength,
                    returnFactor,
                    inOutFactor,
                    randomSeed,
                    progressTracker,
                    terminationFlag
                )).collect(Collectors.toList());

        CompletableFuture.runAsync(
            () -> tasksRunner(
                tasks,
                walks,
                TOMB,
                terminationFlag
            ),
            ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL
        ).whenComplete((__, ___) -> {
            progressTracker.endSubTask("RandomWalk");
        });
    }

    private void tasksRunner(
        Iterable<? extends Runnable> tasks,
        BlockingQueue<long[]> walks,
        long[] tombstone,
        TerminationFlag terminationFlag
    ) {
        progressTracker.beginSubTask("create walks");

        RunWithConcurrency.builder()
            .executor(this.executorService)
            .concurrency(this.concurrency)
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .mayInterruptIfRunning(true)
            .run();

        progressTracker.endSubTask("create walks");

        try {
            boolean finished = false;
            while (!finished && terminationFlag.running()) {
                finished = walks.offer(tombstone, 100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private Stream<long[]> walksQueueConsumer(
        ExternalTerminationFlag terminationFlag,
        long[] tombstone,
        BlockingQueue<long[]> walks
    ) {
        int timeoutInSeconds = 100;
        var queueConsumer = new QueueBasedSpliterator<>(walks, tombstone, terminationFlag, timeoutInSeconds);
        return StreamSupport
            .stream(queueConsumer, false)
            .onClose(terminationFlag::stop);
    }

    private static final class ExternalTerminationFlag implements TerminationFlag {
        private volatile boolean running = true;
        private final Algorithm<?> algo;

        ExternalTerminationFlag(Algorithm<?> algo) {
            this.algo = algo;
        }

        @Override
        public boolean running() {
            return this.running && this.algo.getTerminationFlag().running();
        }

        void stop() {
            this.running = false;
        }
    }

}
