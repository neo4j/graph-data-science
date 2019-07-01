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
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Node;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public class PageRankFactory extends AlgorithmFactory<PageRank> {

    public static final String CONFIG_WEIGHT_KEY = "weightProperty";

    private final PageRank.Config algoConfig;

    public PageRank eigenvectorCentralityOf(Graph graph, LongStream sourceNodeIds) {
        PageRankVariant pageRankVariant = new EigenvectorCentralityVariant();
        return new PageRank(AllocationTracker.EMPTY, graph, algoConfig, sourceNodeIds, pageRankVariant);
    }

    public PageRank weightedOf(Graph graph, LongStream sourceNodeIds) {
        return weightedOf(graph, sourceNodeIds, false, AllocationTracker.EMPTY);
    }

    private PageRank weightedOf(
            Graph graph,
            LongStream sourceNodeIds,
            boolean cacheWeights,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);
        return new PageRank(tracker, graph, algoConfig, sourceNodeIds, pageRankVariant);
    }

    public PageRank articleRankOf(Graph graph, LongStream sourceNodeIds) {
        return articleRankOf(graph, sourceNodeIds, AllocationTracker.EMPTY);
    }

    public PageRank articleRankOf(
            Graph graph,
            LongStream sourceNodeIds,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();
        return new PageRank(tracker, graph, algoConfig, sourceNodeIds, pageRankVariant);
    }

    public PageRank of(Graph graph, LongStream sourceNodeIds) {
        return of(graph, sourceNodeIds, AllocationTracker.EMPTY);
    }

    public PageRank of(Graph graph, LongStream sourceNodeIds, AllocationTracker tracker) {
        PageRankVariant computeStepFactory = new NonWeightedPageRankVariant();
        return new PageRank(tracker, graph, algoConfig, sourceNodeIds, computeStepFactory);
    }

    public PageRank of(
            Graph graph,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize) {
        return of(graph, sourceNodeIds, pool, concurrency, batchSize, AllocationTracker.EMPTY);
    }

    public PageRank of(
            Graph graph,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new NonWeightedPageRankVariant();
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                algoConfig,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public PageRank weightedOf(
            Graph graph,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            boolean cacheWeights,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new WeightedPageRankVariant(cacheWeights);
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                algoConfig,
                sourceNodeIds,
                pageRankVariant
        );
    }

    public PageRank articleRankOf(
            Graph graph,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            AllocationTracker tracker) {
        PageRankVariant pageRankVariant = new ArticleRankVariant();
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                algoConfig,
                sourceNodeIds,
                pageRankVariant
        );

    }

    public PageRank eigenvectorCentralityOf(
            Graph graph,
            LongStream sourceNodeIds,
            ExecutorService pool,
            int concurrency,
            int batchSize,
            AllocationTracker tracker) {
        PageRankVariant variant = new EigenvectorCentralityVariant();
        return new PageRank(
                pool,
                concurrency,
                batchSize,
                tracker,
                graph,
                algoConfig,
                sourceNodeIds,
                variant
        );
    }

    public PageRankFactory(PageRank.Config algoConfig) {
        this.algoConfig = algoConfig;
    }

    @Override
    public PageRank build(final Graph graph, final ProcedureConfiguration configuration, final AllocationTracker tracker) {
        final int batchSize = configuration.getBatchSize();
        final int concurrency = configuration.getConcurrency();
        List<Node> sourceNodes = configuration.get("sourceNodes", Collections.emptyList());
        LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);
        final String weightPropertyKey = configuration.getString(CONFIG_WEIGHT_KEY, null);

        if (weightPropertyKey != null) {
            final boolean cacheWeights = configuration.get("cacheWeights", false);
            return weightedOf(
                    graph,
                    sourceNodeIds,
                    Pools.DEFAULT,
                    concurrency,
                    batchSize,
                    cacheWeights,
                    tracker
            );
        } else {
            return of(
                    graph,
                    sourceNodeIds,
                    Pools.DEFAULT,
                    concurrency,
                    batchSize,
                    tracker
            );
        }
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(PageRank.class)
                .add(MemoryEstimations.setup("computeSteps", (dimensions, concurrency) -> {
                    // adjust concurrency, if necessary
                    long nodeCount = dimensions.nodeCount();
                    long nodesPerThread = ceilDiv(nodeCount, concurrency);
                    if (nodesPerThread > PageRank.Partition.MAX_NODE_COUNT) {
                        concurrency = (int) ceilDiv(nodeCount, PageRank.Partition.MAX_NODE_COUNT);
                        nodesPerThread = ceilDiv(nodeCount, concurrency);
                        while (nodesPerThread > PageRank.Partition.MAX_NODE_COUNT) {
                            concurrency++;
                            nodesPerThread = ceilDiv(nodeCount, concurrency);
                        }
                    }
                    int partitionSize = (int) nodesPerThread;

                    return MemoryEstimations
                            .builder(PageRank.ComputeSteps.class)
                            .perThread("scores[] wrapper", MemoryUsage::sizeOfObjectArray)
                            .perThread("starts[]", MemoryUsage::sizeOfLongArray)
                            .perThread("lengths[]", MemoryUsage::sizeOfLongArray)
                            .perThread("list of computeSteps", MemoryUsage::sizeOfObjectArray)
                            // TODO: Use specific variant instead of BaseComputeStep.class to be more precise on memory requirements
                            .perThread("ComputeStep", BaseComputeStep.estimateMemory(partitionSize, BaseComputeStep.class))
                            .build();
                }))
                .build();
    }
}
