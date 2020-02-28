/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.centrality;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.AlphaAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.pagerank.LabsPageRankAlgorithmType;
import org.neo4j.graphalgo.pagerank.PageRank;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphalgo.utils.CentralityUtils;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public final class ArticleRankProc extends AlgoBaseProc<PageRank, PageRank, ArticleRankConfig> {

    @Procedure(value = "gds.alpha.articleRank.write", mode = WRITE)
    public Stream<PageRankScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, ArticleRankConfig> computationResult = compute(graphNameOrConfig, configuration);

        PageRank algo = computationResult.algorithm();
        ArticleRankConfig config = computationResult.config();
        AllocationTracker tracker = computationResult.tracker();
        Graph graph = computationResult.graph();

        AbstractResultBuilder<PageRankScore.Stats> statsBuilder = new PageRankScore.Stats.Builder()
            .withLoadMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(statsBuilder.build());
        }

        log.info("ArticleRank: overall memory usage: %s", tracker.getUsageString());

        // NOTE: could not use `writeNodeProperties` just yet, as this requires changes to
        //  the Page Rank class and therefore to all product Page Rank procs as well.
        try (ProgressTimer ignored = statsBuilder.timeWrite()) {
            NodePropertyExporter exporter = NodePropertyExporter
                .of(api, graph, algo.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();
            algo.result().export(config.writeProperty(), exporter);
        }

        graph.release();
        return Stream.of(statsBuilder.build());
    }

    @Procedure(value = "gds.alpha.articleRank.stream", mode = READ)
    public Stream<CentralityScore> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, ArticleRankConfig> computationResult = compute(graphNameOrConfig, configuration);

        PageRank algo = computationResult.algorithm();
        AllocationTracker tracker = computationResult.tracker();
        Graph graph = computationResult.graph();

        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }

        log.info("ArticleRank: overall memory usage: %s", tracker.getUsageString());

        return CentralityUtils.streamResults(graph, algo.result());
    }

    @Override
    protected ArticleRankConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return ArticleRankConfig.of(username, graphName, maybeImplicitCreate, userInput);
    }

    @Override
    protected AlgorithmFactory<PageRank, ArticleRankConfig> algorithmFactory(ArticleRankConfig config) {
        return new AlphaAlgorithmFactory<PageRank, ArticleRankConfig>() {
            @Override
            public PageRank build(
                Graph graph,
                ArticleRankConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                PageRank.Config algoConfig = new PageRank.Config(
                    configuration.maxIterations(),
                    configuration.dampingFactor(),
                    configuration.tolerance()
                );
                return LabsPageRankAlgorithmType.ARTICLE_RANK.create(
                    graph,
                    Pools.DEFAULT,
                    configuration.concurrency(),
                    ParallelUtil.DEFAULT_BATCH_SIZE,
                    algoConfig,
                    configuration.sourceNodeIds(),
                    tracker
                );
            }
        };
    }

}
