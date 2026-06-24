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
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.core.utils.queue.QueueBasedSpliterator;
import org.neo4j.gds.logging.Log;
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

public final class RandomWalk implements Algorithm<Stream<long[]>> {
    private static final long[] TOMBSTONE = new long[0];
    private final ProgressTracker progressTracker;

    private final Log log;
    private final TerminationFlag terminationFlag;
    private final Concurrency concurrency;
    private final ExecutorService executorService;
    private final Graph graph;
    private final long randomSeed;
    private final WalkParameters walkParameters;
    private final List<Long> sourceNodes;
    private final ExternalTerminationFlag externalTerminationFlag;
    private final BlockingQueue<long[]> walks;

    public static RandomWalk create(
        Log log,
        Graph graph,
        Concurrency concurrency,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        int walkBufferSize,
        Optional<Long> randomSeed,
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

        return create(
            log,
            graph,
            concurrency,
            executorService,
            walkParameters,
            sourceNodes,
            walkBufferSize,
            randomSeed,
            progressTracker,
            terminationFlag
        );
    }

    public static RandomWalk create(
        Log log,
        Graph graph,
        RandomWalkParameters parameters,
        ProgressTracker progressTracker,
        ExecutorService executorService,
        TerminationFlag terminationFlag
    ) {
        return create(
            log,
            graph,
            parameters.concurrency(),
            executorService,
            parameters.walkParameters(),
            parameters.sourceNodes(),
            parameters.walkBufferSize(),
            parameters.randomSeed(),
            progressTracker,
            terminationFlag
        );
    }

    private static RandomWalk create(
        Log log,
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        int walkBufferSize,
        Optional<Long> maybeRandomSeed,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var walks = new ArrayBlockingQueue<long[]>(walkBufferSize);
        var externalTerminationFlag = new ExternalTerminationFlag(terminationFlag);
        var randomSeed = maybeRandomSeed.orElseGet(() -> new Random().nextLong());

        return new RandomWalk(
            log,
            graph,
            concurrency,
            executorService,
            walkParameters,
            sourceNodes,
            progressTracker,
            terminationFlag,
            walks,
            externalTerminationFlag,
            randomSeed
        );
    }

    private RandomWalk(
        Log log,
        Graph graph,
        Concurrency concurrency,
        ExecutorService executorService,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        ArrayBlockingQueue<long[]> walks,
        ExternalTerminationFlag externalTerminationFlag,
        long randomSeed
    ) {
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        this.log = log;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.graph = graph;
        this.randomSeed = randomSeed;
        this.walkParameters = walkParameters;
        this.sourceNodes = sourceNodes;
        this.walks = walks;
        this.externalTerminationFlag = externalTerminationFlag;
    }

    @Override
    public Stream<long[]> compute() {
        progressTracker.beginSubTask(/*RandomWalk*/);
        var taskSupplier = createRandomWalkTaskSupplier();

        startWalkers(
            taskSupplier,
            progressTracker::endSubTask,
            progressTracker::endSubTaskWithFailure
        );
        return streamWalks(walks);
    }

    private RandomWalkTaskSupplier createRandomWalkTaskSupplier() {
        var nextNodeSupplier = RandomWalkCompanion.nextNodeSupplier(graph, sourceNodes);
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier = RandomWalkCompanion.cumulativeWeights(
            graph,
            concurrency,
            executorService,
            progressTracker,
            terminationFlag
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

    private void startWalkers(RandomWalkTaskSupplier taskSupplier, Runnable onComplete, Runnable onException) {
        var tasks = IntStream
            .range(0, this.concurrency.value())
            .mapToObj(i -> taskSupplier.get())
            .collect(Collectors.toList());

        CompletableFuture.runAsync(
                () -> runTasks(tasks),
                ExecutorServiceUtil.DEFAULT_SINGLE_THREAD_POOL
            )
            .whenComplete((__, throwable) -> {
                    if (throwable != null) {
                        log.info("Failed to create walks: " + throwable.getMessage());
                        onException.run();
                    } else {
                        onComplete.run();
                    }
                }
            );
    }

    private void runTasks(Iterable<? extends Runnable> tasks) {
        progressTracker.beginSubTask(/*create walks*/);

        RunWithConcurrency.builder()
            .executor(this.executorService)
            .concurrency(this.concurrency)
            .tasks(tasks)
            .terminationFlag(this.externalTerminationFlag)
            .mayInterruptIfRunning(true)
            .run();

        progressTracker.endSubTask(/*create walks*/);

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
        var queueConsumer = new QueueBasedSpliterator<>(walks, TOMBSTONE, externalTerminationFlag);
        return StreamSupport
            .stream(queueConsumer, false)
            .onClose(externalTerminationFlag::stop);
    }

    private static final class ExternalTerminationFlag implements TerminationFlag {
        private final TerminationFlag terminationFlag;

        private volatile boolean running = true;

        ExternalTerminationFlag(TerminationFlag terminationFlag) {
            this.terminationFlag = terminationFlag;
        }

        @Override
        public boolean running() {
            return this.running && terminationFlag.running();
        }

        void stop() {
            this.running = false;
        }
    }
}
