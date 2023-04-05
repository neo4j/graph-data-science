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
package org.neo4j.gds.impl.harmonic;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.msbfs.BfsConsumer;
import org.neo4j.gds.msbfs.MultiSourceBFSAccessMethods;

import java.util.concurrent.ExecutorService;

public class HarmonicCentrality extends Algorithm<HarmonicResult> {

    private final int concurrency;
    private final long nodeCount;
    private final ExecutorService executorService;
    private final HugeAtomicDoubleArray inverseFarness;

    private Graph graph;

    public HarmonicCentrality(
        Graph graph,
        int concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.inverseFarness = HugeAtomicDoubleArray.of(graph.nodeCount(), ParallelDoublePageCreator.passThrough(concurrency));
        this.nodeCount = graph.nodeCount();
    }

    @Override
    public HarmonicResult compute() {
        progressTracker.beginSubTask();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            double len = sourceNodeIds.size();
            inverseFarness.update(nodeId, currentValue -> currentValue + (len * (1.0 / depth)));
        };

        MultiSourceBFSAccessMethods.aggregatedNeighborProcessingWithoutSourceNodes(
            graph.nodeCount(),
            graph,
            consumer
        ).run(concurrency, executorService);

        progressTracker.endSubTask();

        return ImmutableHarmonicResult.of(inverseFarness, graph.nodeCount());
    }


}
