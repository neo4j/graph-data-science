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

import static org.neo4j.gds.scaling.ScalarScaler.Variant.L2NORM;
import static org.neo4j.gds.scaling.ScalarScaler.Variant.NONE;
import static org.neo4j.graphalgo.pagerank.PageRankAlgorithmFactory.Mode.EIGENVECTOR;

public class PageRankAlgorithm extends Algorithm<PageRankAlgorithm, PageRankResult> {

    private final Pregel<PageRankConfig> pregelJob;
    private final Graph graph;
    private final PageRankAlgorithmFactory.Mode mode;
    private final PageRankConfig config;
    private final ExecutorService executorService;

    PageRankAlgorithm(
        Graph graph,
        PageRankConfig config,
        PregelComputation<PageRankConfig> pregelComputation,
        PageRankAlgorithmFactory.Mode mode,
        ExecutorService executorService,
        AllocationTracker tracker,
        ProgressLogger progressLogger
    ) {
        this.pregelJob = Pregel.create(graph, config, pregelComputation, executorService, tracker, progressLogger);
        this.mode = mode;
        this.executorService = executorService;
        this.config = config;
        this.graph = graph;
    }

    @Override
    public PageRankResult compute() {
        var pregelResult = pregelJob.run();

        var scores = pregelResult.nodeValues().doubleProperties(PageRankComputation.PAGE_RANK);

        scaleScores(scores, pregelResult.ranIterations());

        return ImmutablePageRankResult.builder()
            .scores(scores)
            .iterations(pregelResult.ranIterations())
            .didConverge(pregelResult.didConverge())
            .build();
    }

    private void scaleScores(HugeDoubleArray scores, int ranIterations) {
        var variant = config.scaler();

        // Eigenvector produces L2NORM-scaled results by default.
        if (variant == NONE || (variant == L2NORM && mode == EIGENVECTOR && ranIterations > 1)) {
            return;
        }

        var scaler = variant.create(
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
