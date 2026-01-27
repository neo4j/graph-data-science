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
package org.neo4j.gds.procedures.algorithms.centrality.write;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.centrality.GenericRankWriteStep;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.procedures.algorithms.WriteStepExecute;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankWriteResult;
import org.neo4j.gds.procedures.algorithms.centrality.RankDistributionHelpers;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.Map;
import java.util.stream.Stream;

public class GenericRankWriteResultTransformer implements ResultTransformer<TimedAlgorithmResult<PageRankResult>, Stream<PageRankWriteResult>> {
    private final Graph graph;
    private final GraphStore graphStore;
    private final ScalerFactory scalerFactory;
    private final Map<String, Object> configuration;
    private final boolean shouldComputeDistribution;
    private final Concurrency concurrency;
    private final GenericRankWriteStep writeStep;
    private final JobId jobId;
    private final ResultStore resultStore;

    public GenericRankWriteResultTransformer(
        Graph graph,
        GraphStore graphStore,
        Map<String, Object> configuration,
        ScalerFactory scalerFactory,
        boolean shouldComputeDistribution,
        Concurrency concurrency,
        GenericRankWriteStep writeStep,
        JobId jobId, ResultStore resultStore
    ) {
        this.graph = graph;
        this.graphStore = graphStore;
        this.configuration = configuration;
        this.scalerFactory = scalerFactory;
        this.shouldComputeDistribution = shouldComputeDistribution;
        this.concurrency = concurrency;
        this.writeStep = writeStep;
        this.jobId = jobId;
        this.resultStore = resultStore;
    }

    @Override
    public Stream<PageRankWriteResult> apply(TimedAlgorithmResult<PageRankResult> timedAlgorithmResult) {
        var result = timedAlgorithmResult.result();
        var centralityDistribution = RankDistributionHelpers.compute(
            graph,
            scalerFactory,
            result.centralityScoreProvider(),
            concurrency,
            shouldComputeDistribution
        );

        var writeMetadata = WriteStepExecute.executeWriteNodePropertyStep(
            writeStep,
            graph,
            graphStore,
            jobId,
            result,
            resultStore
        );

        return Stream.of(
            new PageRankWriteResult(
                result.iterations(),
                result.didConverge(),
                centralityDistribution.centralitySummary(),
                0,
                timedAlgorithmResult.computeMillis(),
                centralityDistribution.computeMillis(),
                writeMetadata.writeMillis(),
                writeMetadata.nodePropertiesWritten().value(),
                configuration
            )
        );
    }
}
