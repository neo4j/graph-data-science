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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.ocd.lhs.LocallyMinimalNeighborhoods;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class ConductanceAffiliationInitializer implements AffiliationInitializer {
    private final KernelTransaction transaction;
    private ExecutorService executorService;
    private final AllocationTracker tracker;
    private final Log log;
    private final int concurrency;
    private final TerminationFlag terminationFlag;

    public ConductanceAffiliationInitializer(
        KernelTransaction transaction,
        ExecutorService executorService,
        AllocationTracker tracker,
        Log log,
        int concurrency
    ) {
        this.transaction = transaction;
        this.executorService = executorService;
        this.tracker = tracker;
        this.log = log;
        this.concurrency = concurrency;
        this.terminationFlag = TerminationFlag.wrap(transaction);
    }

    @Override
    public CommunityAffiliations initialize(Graph graph) {
        LocallyMinimalNeighborhoods locallyMinimalNeighborhoods = new LocallyMinimalNeighborhoods(
            graph,
            log,
            transaction,
            executorService,
            tracker,
            concurrency,
            true
        );
        LocallyMinimalNeighborhoods.Result neighborhoodCommunityResult = locallyMinimalNeighborhoods.compute();
        List<Vector> affiliationVectors = new ArrayList<>((int) graph.nodeCount());
        for (int i = 0; i < graph.nodeCount(); i++) {
            affiliationVectors.add(null);
        }
        AtomicLong queue = new AtomicLong(0);
        AtomicLong totalDoubleEdgeCount = new AtomicLong(0);
        ConcurrentSkipListSet<Long> centers = neighborhoodCommunityResult.neighborhoodCenters;
        Map<Long, Integer> centerMapping = centerMapping(centers);
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            () -> new AffiliationTask(queue, totalDoubleEdgeCount, affiliationVectors,
                centers, centerMapping, graph)
        );
        // run
        ParallelUtil.run(tasks, executorService);
        return new CommunityAffiliations(totalDoubleEdgeCount.get(), affiliationVectors, graph);
    }

    private Map<Long, Integer> centerMapping(ConcurrentSkipListSet<Long> centers) {
        Map<Long, Integer> centerIds = new HashMap<>();
        int mapped = 0;
        for (long center : centers) {
            centerIds.put(center, mapped);
            mapped++;
        }
        return centerIds;
    }

    class AffiliationTask implements Runnable {
        private final AtomicLong queue;
        private final AtomicLong totalDoubleEdgeCount;
        private final List<Vector> affiliationVectors;
        private final ConcurrentSkipListSet<Long> centers;
        private final Map<Long, Integer> centerMapping;
        private final Graph graph;

        AffiliationTask(
            AtomicLong queue,
            AtomicLong totalDoubleEdgeCount,
            List<Vector> affiliationVectors,
            ConcurrentSkipListSet<Long> centers,
            Map<Long, Integer> centerMapping,
            Graph graph
        ) {
            this.queue = queue;
            this.totalDoubleEdgeCount = totalDoubleEdgeCount;
            this.affiliationVectors = affiliationVectors;
            this.centers = centers;
            this.centerMapping = centerMapping;
            this.graph = graph;
        }

        @Override
        public void run() {
            long nodeId;
            int dimension = centers.size();
            while ((nodeId = queue.getAndIncrement()) < graph.nodeCount() && terminationFlag.running()) {
                totalDoubleEdgeCount.addAndGet(graph.degree(nodeId));
                double[] values = new double[dimension];
                if (centers.contains(nodeId)) {
                    values[centerMapping.get(nodeId)] = 1D;
                }
                graph.concurrentCopy().forEachRelationship(nodeId, (src, trg) -> {
                    if (centers.contains(trg)) {
                        values[centerMapping.get(trg)] = 1D;
                    }
                    return true;
                });
                affiliationVectors.set((int)nodeId, new Vector(values));
            }
        }
    }
}
