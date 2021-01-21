/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfFloatArray;

public interface PageRankAlgorithm {

    /**
     * Forces sequential use. If you want parallelism, prefer
     *
     * {@link #create(org.neo4j.graphalgo.api.Graph, java.util.stream.LongStream, PageRankBaseConfig, java.util.concurrent.ExecutorService, org.neo4j.graphalgo.core.utils.ProgressLogger, org.neo4j.graphalgo.core.utils.mem.AllocationTracker)} }
     */
    default PageRank create(
        Graph graph,
        PageRankBaseConfig algoConfig,
        LongStream sourceNodeIds,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        return create(graph, sourceNodeIds, algoConfig, null, progressLogger, tracker);
    }

    default PageRank create(
        Graph graph,
        LongStream sourceNodeIds,
        PageRankBaseConfig algoConfig,
        ExecutorService executor,
        ProgressLogger progressLogger,
        AllocationTracker tracker
    ) {
        return new PageRank(
            graph,
            variant(),
            sourceNodeIds,
            algoConfig,
            executor,
            progressLogger,
            tracker
        );
    }

    PageRankVariant variant();

    Class<? extends BaseComputeStep> computeStepClass();

    default MemoryEstimation memoryEstimation(long partitionCount, long nodesPerPartition) {
        return MemoryEstimations.builder(computeStepClass())
            .fixed("nextScores[] wrapper", MemoryUsage.sizeOfObjectArray(partitionCount))
            .fixed("inner nextScores[][]", sizeOfFloatArray(nodesPerPartition) * partitionCount)
            .fixed("pageRank[]", sizeOfDoubleArray(nodesPerPartition))
            .fixed("deltas[]", sizeOfDoubleArray(nodesPerPartition))
            .build()
            .times(partitionCount);
    }
}
