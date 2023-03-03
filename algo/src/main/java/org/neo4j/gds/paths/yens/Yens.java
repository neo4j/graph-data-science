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
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.paths.yens.config.ImmutableShortestPathYensBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class Yens extends Algorithm<DijkstraResult> {

    static final LongHashSet EMPTY_SET = new LongHashSet(0);

    private final Graph graph;
    private final ShortestPathYensBaseConfig config;
    private final Dijkstra dijkstra;



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
        // Init dijkstra algorithm for computing shortest paths
        var dijkstra = Dijkstra.sourceTarget(graph, newConfig, Optional.empty(), progressTracker);
        return new Yens(graph, dijkstra, newConfig, progressTracker);
    }

    // The blacklists contain nodes and relationships that are
    // "forbidden" to be traversed by Dijkstra. The size of that
    // blacklist is not known upfront and depends on the length
    // of the found paths.
    private static final long AVERAGE_BLACKLIST_SIZE = 10L;

    public static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(Yens.class.getSimpleName())
            .add("Dijkstra", Dijkstra.memoryEstimation(false))
            .fixed("nodeBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE))
            .fixed("relationshipBlackList", MemoryUsage.sizeOfLongArray(AVERAGE_BLACKLIST_SIZE * 2))
            .build();
    }

    private Yens(Graph graph, Dijkstra dijkstra, ShortestPathYensBaseConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;

        // set filter in Dijkstra to respect our list of relationships to avoid
        this.dijkstra = dijkstra;

    }



    @Override
    public DijkstraResult compute() {
        progressTracker.beginSubTask();
        var kShortestPaths = new ArrayList<MutablePathResult>();
        // compute top 1 shortest path
        progressTracker.beginSubTask();
        progressTracker.beginSubTask();
        var shortestPath = computeDijkstra(config.sourceNode());

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
            currentSpurIndexId.set(0);
            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(tasks)
                .executor(Pools.DEFAULT)
                .run();

            if (candidates.isEmpty()) {
                break;
            }
            addPathToSolution(i, kShortestPaths, candidates);
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();

        progressTracker.endSubTask();

        return new DijkstraResult(kShortestPaths.stream().map(MutablePathResult::toPathResult));
    }

    private void addPathToSolution(
        int index,
        ArrayList<MutablePathResult> kShortestPaths,
        PriorityQueue<MutablePathResult> candidates
    ) {
        var pathToAdd = candidates.poll();
        pathToAdd.withIndex(index);
        kShortestPaths.add(pathToAdd);
    }

    @NotNull
    private PriorityQueue<MutablePathResult> initCandidatesQueue() {
        return new PriorityQueue<>(Comparator
            .comparingDouble(MutablePathResult::totalCost)
            .thenComparingInt(MutablePathResult::nodeCount));
    }

    private Optional<PathResult> computeDijkstra(long sourceNode) {
        progressTracker.logInfo(formatWithLocale("Dijkstra for spur node %d", sourceNode));
        return dijkstra.compute().findFirst();
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
                config.trackRelationships()
            ));
        }
        return tasks;
    }

}


