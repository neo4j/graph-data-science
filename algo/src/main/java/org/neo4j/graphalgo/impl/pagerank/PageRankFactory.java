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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.pagerank.PageRankConfigBase;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;

import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;
import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

public class PageRankFactory<CONFIG extends PageRankConfigBase> extends AlgorithmFactory<PageRank, CONFIG> {

    private final PageRankAlgorithmType algorithmType;
    private final PageRank.Config algoConfig;

    public PageRankFactory(PageRankConfigBase config) {
        this(PageRankAlgorithmType.NON_WEIGHTED, config);
    }

    public PageRankFactory(PageRankAlgorithmType algorithmType, PageRankConfigBase config) {
        this.algorithmType = algorithmType;
        this.algoConfig = config.toOldConfig();
    }

    @Override
    public PageRank build(
            final Graph graph,
            final PageRankConfigBase configuration,
            final AllocationTracker tracker,
            final Log log) {

        LongStream sourceNodeIds = configuration.sourceNodeIds().stream().mapToLong(Node::getId);
        return algorithmType.create(
                graph,
                Pools.DEFAULT,
                DEFAULT_BATCH_SIZE,
                configuration.concurrency(),
                algoConfig,
                sourceNodeIds,
                tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(PageRank.class)
                .add(MemoryEstimations.setup("computeSteps", (dimensions, concurrency) -> {
                    // adjust concurrency, if necessary
                    long nodeCount = dimensions.nodeCount();
                    long nodesPerThread = ceilDiv(nodeCount, concurrency);
                    if (nodesPerThread > Partition.MAX_NODE_COUNT) {
                        concurrency = (int) ceilDiv(nodeCount, Partition.MAX_NODE_COUNT);
                        nodesPerThread = ceilDiv(nodeCount, concurrency);
                        while (nodesPerThread > Partition.MAX_NODE_COUNT) {
                            concurrency++;
                            nodesPerThread = ceilDiv(nodeCount, concurrency);
                        }
                    }

                    return MemoryEstimations
                            .builder(PageRank.ComputeSteps.class)
                            .perThread("scores[] wrapper", MemoryUsage::sizeOfObjectArray)
                            .perThread("starts[]", MemoryUsage::sizeOfLongArray)
                            .perThread("lengths[]", MemoryUsage::sizeOfLongArray)
                            .perThread("list of computeSteps", MemoryUsage::sizeOfObjectArray)
                            .perThread("ComputeStep", algorithmType.memoryEstimation())
                            .build();
                }))
                .build();
    }
}
