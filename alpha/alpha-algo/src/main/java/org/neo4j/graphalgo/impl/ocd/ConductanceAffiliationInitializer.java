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
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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

    ConductanceAffiliationInitializer(
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
        List<SparseVector> affiliationVectors = new ArrayList<>((int) graph.nodeCount());
        AtomicLong queue = new AtomicLong(0);
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(
            concurrency,
            () -> new AffiliationTask(queue, affiliationVectors, neighborhoodCommunityResult.neighborhoodCenters, graph)
        );
        // run
        ParallelUtil.run(tasks, executorService);
        return new CommunityAffiliations(affiliationVectors, graph);
    }

    class AffiliationTask implements Runnable {
        private final AtomicLong queue;
        private final List<SparseVector> affiliationVectors;
        private final ConcurrentSkipListSet<Long> centers;
        private final Graph graph;

        AffiliationTask(AtomicLong queue, List<SparseVector> affiliationVectors, ConcurrentSkipListSet<Long> centers, Graph graph) {
            this.queue = queue;
            this.affiliationVectors = affiliationVectors;
            this.centers = centers;
            this.graph = graph;
        }

        @Override
        public void run() {
            long nodeId;
            while ((nodeId = queue.getAndIncrement()) < graph.nodeCount() && terminationFlag.running()) {
                List<Integer> indices = new LinkedList<>();
                List<Double> values = new LinkedList<>();
                graph.forEachRelationship(nodeId, (src, trg) -> {
                    if (centers.contains(trg)) {
                        indices.add((int)trg);
                        values.add(1D);
                    }
                    return true;
                });
                SparseVector affiliationVector = SparseVector.getSparseVectorFromLists(indices, values);
                affiliationVectors.set((int)nodeId, affiliationVector);
            }
        }
    }
}
