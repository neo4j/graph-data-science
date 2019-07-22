/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.Assessable;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public interface PageRankAlgorithm extends Assessable {

    /**
     * Forces sequential use. If you want parallelism, prefer
     *
     * {@link #create(Graph, ExecutorService, int, int, PageRank.Config, LongStream, AllocationTracker)} }
     */
    default PageRank create(
            Graph graph,
            PageRank.Config algoConfig,
            LongStream sourceNodeIds) {
        return create(graph, algoConfig, sourceNodeIds, AllocationTracker.EMPTY);
    }

    /**
     * Forces sequential use. If you want parallelism, prefer
     *
     * * {@link #create(Graph, ExecutorService, int, int, PageRank.Config, LongStream, AllocationTracker)} }
     */
    default PageRank create(
            Graph graph,
            PageRank.Config algoConfig,
            LongStream sourceNodeIds,
            AllocationTracker tracker) {
        return create(graph, null, ParallelUtil.DEFAULT_BATCH_SIZE, -1, algoConfig, sourceNodeIds, tracker);
    }

    default PageRank create(
            Graph graph,
            ExecutorService executor,
            int batchSize,
            int concurrency,
            PageRank.Config algoConfig,
            LongStream sourceNodeIds) {
        return create(graph, executor, batchSize, concurrency, algoConfig, sourceNodeIds, AllocationTracker.EMPTY);
    }

    default PageRank create(
            Graph graph,
            ExecutorService executor,
            int batchSize,
            int concurrency,
            PageRank.Config algoConfig,
            LongStream sourceNodeIds,
            AllocationTracker tracker) {
        return new PageRank(executor, concurrency, batchSize, tracker, graph, algoConfig, sourceNodeIds, variant(algoConfig));
    }

    PageRankVariant variant(PageRank.Config config);

    Class<? extends BaseComputeStep> computeStepClass();

    @Override
    default MemoryEstimation memoryEstimation() {
        return MemoryEstimations.setup("ComputeStep", (dimensions, concurrency) -> {
            long nodeCount = dimensions.nodeCount();
            long nodesPerThread = ceilDiv(nodeCount, concurrency);
            return BaseComputeStep.estimateMemory((int) nodesPerThread, computeStepClass());
        });
    }
}
