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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.gds.scaling.ScalarScaler;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.concurrent.ExecutorService;

public class PageRankAlgorithm extends Algorithm<PageRankAlgorithm, PageRankResult> {

    private final Pregel<PageRankConfig> pregelJob;
    private final Graph graph;
    private final PageRankConfig config;
    private final ExecutorService executorService;

    PageRankAlgorithm(
        Graph graph,
        PageRankConfig config,
        PregelComputation<PageRankConfig> pregelComputation,
        ExecutorService executorService,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.pregelJob = Pregel.create(graph, config, pregelComputation, executorService, tracker, progressLogger);
        this.executorService = executorService;
        this.config = config;
        this.graph = graph;
    }

    @Override
    public PageRankResult compute() {
        var pregelResult = pregelJob.run();

        HugeDoubleArray scores = pregelResult.nodeValues().doubleProperties(PageRankComputation.PAGE_RANK);

        normalizeScores(scores);

        return ImmutablePageRankResult.builder()
            .scores(scores)
            .iterations(pregelResult.ranIterations())
            .didConverge(pregelResult.didConverge())
            .build();
    }

    private void normalizeScores(HugeDoubleArray scores) {
        var normalization = config.normalization();
        if (normalization == ScalarScaler.Variant.NONE) {
            return;
        }

        var scaler = normalization.create(
            scores.asNodeProperties(),
            graph.nodeCount(),
            config.concurrency(),
            executorService
        );

        var tasks = PartitionUtils.rangePartition(config.concurrency(), graph.nodeCount(),
            partition -> (Runnable) () -> partition.consume(nodeId -> scores.set(nodeId, scaler.scaleProperty(nodeId)))
        );

        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executorService);
    }

    @Override
    public PageRankAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
        this.pregelJob.release();
    }
}
