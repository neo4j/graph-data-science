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
package org.neo4j.graphalgo.centrality.eigenvector;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.utils.NormalizationFunction;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.pagerank.PageRank;
import org.neo4j.graphalgo.results.AbstractResultBuilder;
import org.neo4j.graphalgo.results.CentralityResult;
import org.neo4j.graphalgo.results.CentralityResultWithStatistics;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphalgo.utils.CentralityUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.Mode.WRITE;

public final class EigenvectorCentralityProc extends AlgoBaseProc<PageRank, PageRank, EigenvectorCentralityConfig> {
    private static final String DESCRIPTION = "Eigenvector Centrality measures the transitive influence or connectivity of nodes.";

    @Procedure(value = "gds.alpha.eigenvector.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<PageRankScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, EigenvectorCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        PageRank algorithm = computationResult.algorithm();
        Graph graph = computationResult.graph();
        CentralityResultWithStatistics stats = CentralityResultWithStatistics.of(algorithm.result(), computationResult.config().concurrency());
        EigenvectorCentralityConfig config = computationResult.config();
        CentralityResult normalizedResults = normalization(config.normalization()).apply(stats);

        AbstractResultBuilder<PageRankScore.Stats> statsBuilder = new PageRankScore.Stats.Builder()
            .withIterations(algorithm.iterations())
            .withDampingFactor(algorithm.dampingFactor())
            .withWriteProperty(config.writeProperty())
            .withCreateMillis(computationResult.createMillis())
            .withComputeMillis(computationResult.computeMillis());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(statsBuilder.build());
        }

        // NOTE: could not use `writeNodeProperties` just yet, as this requires changes to
        //  the Page Rank class and therefore to all product Page Rank procs as well.
        try (ProgressTimer ignored = statsBuilder.timeWrite()) {
            NodePropertyExporter exporter = NodePropertyExporter
                .of(api, computationResult.graph(), algorithm.getTerminationFlag())
                .withLog(log)
                .parallel(Pools.DEFAULT, config.writeConcurrency())
                .build();
            normalizedResults.export(config.writeProperty(), exporter);
        }

        graph.release();
        return Stream.of(statsBuilder.build());
    }

    @Procedure(name = "gds.alpha.eigenvector.stream", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<CentralityScore> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, EigenvectorCentralityConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        CentralityResultWithStatistics centralityResult = CentralityResultWithStatistics.of(computationResult.result().result(), computationResult.config().concurrency());
        String normalization = computationResult.config().normalization();
        return CentralityUtils.streamResults(computationResult.graph(), normalization(normalization).apply(centralityResult));
    }


    @Override
    protected AlgorithmFactory<PageRank, EigenvectorCentralityConfig> algorithmFactory(EigenvectorCentralityConfig config) {
        return new EigenvectorCentralityAlgorithmFactory();
    }

    @Override
    protected EigenvectorCentralityConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return EigenvectorCentralityConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    private NormalizationFunction normalization(String normalization) {
        return NormalizationFunction.valueOf(normalization.toUpperCase());
    }

}
