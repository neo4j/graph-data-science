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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.scaling.L2Norm;
import org.neo4j.gds.scaling.NoneScaler;

import java.util.Optional;
import java.util.concurrent.ExecutorService;


public class PageRankAlgorithm extends Algorithm<PageRankResult> {

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
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.pregelJob = Pregel.create(graph, config, pregelComputation, executorService, progressTracker);
        this.mode = mode;
        this.executorService = executorService;
        this.config = config;
        this.graph = graph;
    }

    @Override
    public void setTerminationFlag(TerminationFlag terminationFlag) {
        super.setTerminationFlag(terminationFlag);
        pregelJob.setTerminationFlag(terminationFlag);
    }

    @Override
    public PageRankResult compute() {
        var pregelResult = pregelJob.run();

        var scores = pregelResult.nodeValues().doubleProperties(PageRankComputation.PAGE_RANK);

        scaleScores(scores);

        return ImmutablePageRankResult.builder()
            .scores(scores)
            .iterations(pregelResult.ranIterations())
            .didConverge(pregelResult.didConverge())
            .build();
    }

    private void scaleScores(HugeDoubleArray scores) {
        var scalerFactory = config.scaler();

        // Eigenvector produces L2NORM-scaled results by default.
        if (scalerFactory.type().equals(NoneScaler.TYPE) || (scalerFactory.type().equals(L2Norm.TYPE) && mode == PageRankAlgorithmFactory.Mode.EIGENVECTOR)) {
            return;
        }

        var scaler = scalerFactory.create(
            scores.asNodeProperties(),
            graph.nodeCount(),
            config.concurrency(),
            ProgressTracker.NULL_TRACKER,
            executorService
        );

        var tasks = PartitionUtils.rangePartition(config.concurrency(), graph.nodeCount(),
            partition -> (Runnable) () -> partition.consume(nodeId -> scores.set(nodeId, scaler.scaleProperty(nodeId))),
            Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executorService)
            .run();
    }

}
