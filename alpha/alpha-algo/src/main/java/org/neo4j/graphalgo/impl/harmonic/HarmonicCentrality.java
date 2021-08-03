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
package org.neo4j.graphalgo.impl.harmonic;

import org.neo4j.gds.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;

import java.util.concurrent.ExecutorService;

public class HarmonicCentrality extends Algorithm<HarmonicCentrality, HarmonicCentrality> {

    private final int concurrency;
    private final long nodeCount;
    private final AllocationTracker allocationTracker;
    private final ExecutorService executorService;
    private final HugeAtomicDoubleArray inverseFarness;

    private Graph graph;

    public HarmonicCentrality(
        Graph graph,
        AllocationTracker allocationTracker,
        int concurrency,
        ExecutorService executorService
    ) {
        this.graph = graph;
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        this.executorService = executorService;
        inverseFarness = HugeAtomicDoubleArray.newArray(graph.nodeCount(), allocationTracker);
        this.nodeCount = graph.nodeCount();
    }

    @Override
    public HarmonicCentrality compute() {
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            double len = sourceNodeIds.size();
            inverseFarness.update(nodeId, currentValue -> currentValue + (len * (1.0 / depth)));
        };

        MultiSourceBFS.aggregatedNeighborProcessing(
            graph,
            graph,
            consumer,
            allocationTracker
        ).run(concurrency, executorService);

        return this;
    }

    @Override
    public HarmonicCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    public double getCentralityScore(long nodeId) {
        return inverseFarness.get(nodeId) / (double) (nodeCount - 1);
    }
}
