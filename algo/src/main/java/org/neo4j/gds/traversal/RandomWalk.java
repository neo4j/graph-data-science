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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;
import org.neo4j.gds.termination.TerminationFlag;

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

    private static final long[] TOMBSTONE = new long[0];

    private final Concurrency concurrency;
    private final ExecutorService executorService;
    private final Graph graph;
    private final long randomSeed;
    private final WalkParameters walkParameters;
    private final List<Long> sourceNodes;
    private final ExternalTerminationFlag externalTerminationFlag;
    private final BlockingQueue<long[]> walks;

    public static RandomWalk create(
        Graph graph,
        Concurrency concurrency,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        int walkBufferSize,
        Optional<Long> randomSeed,
        ProgressTracker progressTracker,
        ExecutorService executorService
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

        return new RandomWalk(
            graph,
            concurrency,
            executorService,
            walkParameters,
            sourceNodes,
            walkBufferSize,
            randomSeed,
            progressTracker
        );
    }

    private RandomWalk(
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        int walkBufferSize,
        Optional<Long> maybeRandomSeed,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.walks = new ArrayBlockingQueue<>(walkBufferSize);
        this.externalTerminationFlag = new ExternalTerminationFlag(this);
        this.graph = graph;
        this.walkParameters = walkParameters;
        this.sourceNodes = sourceNodes;
        this.randomSeed = maybeRandomSeed.orElseGet(() -> new Random().nextLong());
    }

    @Override
    public Stream<long[]> compute() {
        progressTracker.beginSubTask("RandomWalk");
        var taskSupplier = createRandomWalkTaskSupplier();

        startWalkers(
            taskSupplier,
            () -> progressTracker.endSubTask("RandomWalk")
        );
        return streamWalks(walks);
    }

    RandomWalkTaskSupplier createRandomWalkTaskSupplier() {
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
            walks,
            walkParameters,
            randomSeed,
            progressTracker,
            externalTerminationFlag
        );
    }

    private void startWalkers(RandomWalkTaskSupplier taskSupplier, Runnable whenCompleteAction) {
        var tasks = IntStream
            .range(0, this.concurrency.value())
            .mapToObj(i -> taskSupplier.get())
            .collect(Collectors.toList());

        CompletableFuture.runAsync(
            () -> runTasks(tasks),
            ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL
        ).whenComplete((__, ___) -> whenCompleteAction.run());
    }

    private void runTasks(Iterable<? extends Runnable> tasks) {
        progressTracker.beginSubTask("create walks");

        RunWithConcurrency.builder()
            .executor(this.executorService)
            .concurrency(this.concurrency)
            .tasks(tasks)
            .terminationFlag(this.externalTerminationFlag)
            .mayInterruptIfRunning(true)
            .run();

        progressTracker.endSubTask("create walks");

        try {
            boolean finished = false;
            while (!finished && externalTerminationFlag.running()) {
                finished = walks.offer(TOMBSTONE, 100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private Stream<long[]> streamWalks(BlockingQueue<long[]> walks) {
        int timeoutInSeconds = 100;
        var queueConsumer = new QueueBasedSpliterator<>(walks, TOMBSTONE, externalTerminationFlag, timeoutInSeconds);
        return StreamSupport
            .stream(queueConsumer, false)
            .onClose(externalTerminationFlag::stop);
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
