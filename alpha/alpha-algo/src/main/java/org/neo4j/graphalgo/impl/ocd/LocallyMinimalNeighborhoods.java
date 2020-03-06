/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.ocd;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class LocallyMinimalNeighborhoods extends Algorithm<LocallyMinimalNeighborhoods, LocallyMinimalNeighborhoods.Result> {
    private Graph graph;
    private final KernelTransaction transaction;
    private ExecutorService executorService;
    private final AllocationTracker tracker;
    private final Log log;
    private final int concurrency;
    private final boolean includeMembers;

    public LocallyMinimalNeighborhoods(
        Graph graph,
        Log log,
        KernelTransaction transaction,
        ExecutorService executorService,
        AllocationTracker tracker,
        int concurrency,
        boolean includeMembers
    ) {
        this.graph = graph;
        this.log = log;
        this.transaction = transaction;
        this.executorService = executorService;
        this.tracker = tracker;
        this.concurrency = concurrency;
        this.includeMembers = includeMembers;
        if (graph.nodeCount() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("LocallyMinimalNeighborhoods only supports graphs with 2^32-1 nodes.");
        }
    }

    @Override
    public Result compute() {
        IntersectingTriangleCount intersectingTriangleCount = new IntersectingTriangleCount(
            graph,
            Pools.DEFAULT,
            concurrency,
            tracker
        )
        .withProgressLogger(ProgressLogger.wrap(log, "TriangleCount"))
        .withTerminationFlag(TerminationFlag.wrap(transaction));
        PagedAtomicIntegerArray triangleCounts = intersectingTriangleCount.compute();
        PagedAtomicDoubleArray conductances = computeConductances(triangleCounts);
        return postProcess(conductances);
    }

    private Result postProcess(PagedAtomicDoubleArray conductances) {
        ConcurrentSkipListSet<Long> neighborhoodCenters = new ConcurrentSkipListSet<>();
        ConcurrentMap<Long, LongSet> communityMemberships = includeMembers ? new ConcurrentHashMap<>((int) (graph.nodeCount() / 200)) : null;
        AtomicLong queue = new AtomicLong(0);
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, () -> new PostProcessingTask(queue, neighborhoodCenters, communityMemberships, conductances));
        // run
        ParallelUtil.run(tasks, executorService);
        return new Result(neighborhoodCenters, communityMemberships, conductances);
    }

    private PagedAtomicDoubleArray computeConductances(PagedAtomicIntegerArray triangleCounts) {
        AtomicLong queue = new AtomicLong(0);
        PagedAtomicDoubleArray conductances = PagedAtomicDoubleArray.newArray(graph.nodeCount(), tracker);
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, () -> new ConductancesTask(queue, triangleCounts, conductances));
        // run
        ParallelUtil.run(tasks, executorService);
        return conductances;
    }

    @Override
    public LocallyMinimalNeighborhoods me() {
        return this;
    }

    @Override
    public void release() {
        this.graph = null;
        this.executorService = null;
    }

    public static class Result {
        public final ConcurrentSkipListSet<Long> neighborhoodCenters;
        public final @Nullable ConcurrentMap<Long, LongSet> communityMemberships;
        public final PagedAtomicDoubleArray conductances;

        Result(
            ConcurrentSkipListSet<Long> neighborhoodCenters,
            @Nullable ConcurrentMap<Long, LongSet> communityMemberships,
            PagedAtomicDoubleArray conductances
        ) {
            this.neighborhoodCenters = neighborhoodCenters;
            this.communityMemberships = communityMemberships;
            this.conductances = conductances;
        }
    }

    private class ConductancesTask implements Runnable {
        private final AtomicLong queue;
        private final PagedAtomicIntegerArray triangleCounts;
        private final PagedAtomicDoubleArray conductances;

        ConductancesTask(
            AtomicLong queue,
            PagedAtomicIntegerArray triangleCounts,
            PagedAtomicDoubleArray conductances
        ) {
            this.queue = queue;
            this.triangleCounts = triangleCounts;
            this.conductances = conductances;
        }

        @Override
        public void run() {
            long nodeId;
            long nodeCount = graph.nodeCount();
            while ((nodeId = queue.getAndIncrement()) < graph.nodeCount() && running()) {
                long[] cut = new long[1];
                int degree = graph.degree(nodeId);
                cut[0] = -degree - 2 * triangleCounts.get(nodeId);
                graph.concurrentCopy().forEachRelationship(nodeId, (s, t) -> {
                    cut[0] += graph.degree(t);
                    return true;
                });
                long volume = degree < nodeCount / 2 ? degree + 1 : nodeCount - degree - 1;
                conductances.set(nodeId, quotient(cut[0], volume));
            }
        }

        private double quotient(long cut, long volume) {
            if (volume == 0) {
                return 0;
            }
            return BigDecimal.valueOf(cut).divide(BigDecimal.valueOf(volume), 6, RoundingMode.FLOOR).doubleValue();
        }
    }

    private class PostProcessingTask implements Runnable {
        private final AtomicLong queue;
        private final ConcurrentSkipListSet<Long> neighborhoodCenters;
        private final ConcurrentMap<Long, LongSet> communityMemberships;
        private final PagedAtomicDoubleArray conductances;

        public PostProcessingTask(
            AtomicLong queue,
            ConcurrentSkipListSet<Long> neighborhoodCenters,
            ConcurrentMap<Long, LongSet> communityMemberships,
            PagedAtomicDoubleArray conductances
        ) {
            this.queue = queue;
            this.neighborhoodCenters = neighborhoodCenters;
            this.communityMemberships = communityMemberships;
            this.conductances = conductances;
        }

        @Override
        public void run() {
            long nodeId;
            while ((nodeId = queue.getAndIncrement()) < graph.nodeCount() && running()) {
                double conductance = conductances.get(nodeId);
                double[] minimumConductance = { conductance };
                graph.concurrentCopy().forEachRelationship(nodeId, (s, t) -> {
                    minimumConductance[0] = Math.min(minimumConductance[0], conductances.get(t));
                    return true;
                });
                if (conductance <= minimumConductance[0]) {
                    long originalNodeId = graph.toOriginalNodeId(nodeId);
                    neighborhoodCenters.add(originalNodeId);
                    if (includeMembers) {
                        LongSet members = new LongHashSet(graph.degree(originalNodeId));
                        communityMemberships.put(originalNodeId, members);
                        members.add(originalNodeId);
                        graph.concurrentCopy().forEachRelationship(originalNodeId, (s, t) -> {
                            members.add(graph.toOriginalNodeId(t));
                            return true;
                        });
                    }
                }
            }
        }
    }
}
