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
package org.neo4j.gds.paths.yens;

import com.carrotsearch.hppc.LongHashSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.ShortestPathBaseConfig;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.dijkstra.config.ImmutableShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.yens.config.ImmutableShortestPathYensBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public final class Yens extends Algorithm<DijkstraResult> {

    static final LongHashSet EMPTY_SET = new LongHashSet(0);

    private final Graph graph;
    private final ShortestPathYensBaseConfig config;

    /**
     * Configure Yens to compute at most one source-target shortest path.
     */
    public static Yens sourceTarget(
        Graph graph,
        ShortestPathYensBaseConfig config,
        ProgressTracker progressTracker
    ) {
        // If the input graph is a multi-graph, we need to track
        // parallel relationships ids. This is necessary since shortest
        // paths can visit the same nodes via different relationships.
        //If not, we need to track which is the next neighbor.

        boolean shouldTrackRelationships = graph.isMultiGraph();
        var newConfig = ImmutableShortestPathYensBaseConfig
            .builder()
            .from(config)
            .trackRelationships(shouldTrackRelationships)
            .build();

        return new Yens(graph, newConfig, progressTracker);
    }

    public static MemoryEstimation memoryEstimation(int k, boolean trackRelationships) {
        return MemoryEstimations.builder(Yens.class)
            .perThread("Yens Task", YensTask.memoryEstimation(k, trackRelationships))
            .build();
    }

    private Yens(Graph graph, ShortestPathYensBaseConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;

    }


    @Override
    public DijkstraResult compute() {
        progressTracker.beginSubTask();
        var kShortestPaths = new ArrayList<MutablePathResult>();
        // compute top 1 shortest path
        progressTracker.beginSubTask();
        progressTracker.beginSubTask();

        var shortestPath = findFirstPath();

        // no shortest path has been found
        if (shortestPath.isEmpty()) {
            progressTracker.endSubTask();
            progressTracker.endSubTask();
            return new DijkstraResult(Stream.empty(), progressTracker::endSubTask);
        }

        progressTracker.endSubTask();

        kShortestPaths.add(MutablePathResult.of(shortestPath.get()));

        PriorityQueue<MutablePathResult> candidates = initCandidatesQueue();

        AtomicInteger currentSpurIndexId = new AtomicInteger(0);

        var candidateLock = new ReentrantLock();
        var tasks = createTasks(kShortestPaths, candidates, candidateLock, currentSpurIndexId);

        for (int i = 1; i < config.k(); i++) {
            progressTracker.beginSubTask();
            var prevPath = kShortestPaths.get(i - 1);
            for (var task : tasks) {
                task.withPreviousPath(prevPath);
            }

            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(tasks)
                .executor(Pools.DEFAULT)
                .run();

            if (candidates.isEmpty()) {
                break;
            }
            addPathToSolution(i, kShortestPaths, candidates, currentSpurIndexId);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();

        progressTracker.endSubTask();

        return new DijkstraResult(kShortestPaths.stream().map(MutablePathResult::toPathResult));
    }

    private void addPathToSolution(
        int index,
        ArrayList<MutablePathResult> kShortestPaths,
        PriorityQueue<MutablePathResult> candidates,
        AtomicInteger currentSpurIndexId
    ) {
        var pathToAdd = candidates.poll();
        int newIndex = (int) pathToAdd.index();
        pathToAdd.withIndex(index);
        kShortestPaths.add(pathToAdd);
        currentSpurIndexId.set(newIndex);
    }

    @NotNull
    private PriorityQueue<MutablePathResult> initCandidatesQueue() {
        return new PriorityQueue<>(Comparator
            .comparingDouble(MutablePathResult::totalCost)
            .thenComparingInt(MutablePathResult::nodeCount));
    }

    private ArrayList<YensTask> createTasks(
        ArrayList<MutablePathResult> kShortestPaths,
        PriorityQueue<MutablePathResult> candidates,
        ReentrantLock candidateLock,
        AtomicInteger currentSpurIndexId
    ) {
        var tasks = new ArrayList<YensTask>();
        for (int concurrentId = 0; concurrentId < config.concurrency(); ++concurrentId) {
            tasks.add(new YensTask(
                graph.concurrentCopy(),
                config.targetNode(),
                kShortestPaths,
                candidateLock,
                candidates,
                currentSpurIndexId,
                config.trackRelationships(),
                config.k()
            ));
        }
        return tasks;
    }

    private Optional<PathResult> findFirstPath() {

        var dijkstra = Dijkstra.sourceTarget(
            graph,
            config,
            Optional.empty(),
            ProgressTracker.NULL_TRACKER
        );
        var result = dijkstra.compute();
        return result.findFirst();
    }

    static ShortestPathBaseConfig dijkstraConfig(long targetNode, boolean trackRelationships) {

        return ImmutableShortestPathDijkstraStreamConfig
            .builder()
            .sourceNode(targetNode) //this irrelevant
            .targetNode(targetNode)
            .trackRelationships(trackRelationships)
            .build();
    }

}


