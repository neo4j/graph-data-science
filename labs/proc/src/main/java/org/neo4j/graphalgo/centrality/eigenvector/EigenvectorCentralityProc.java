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
package org.neo4j.graphalgo.centrality.eigenvector;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.pagerank.LabsPageRankAlgorithmType;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphalgo.impl.utils.NormalizationFunction;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.CentralityResultWithStatistics;
import org.neo4j.graphalgo.results.CentralityScore;
import org.neo4j.graphalgo.results.PageRankScore;
import org.neo4j.graphalgo.utils.CentralityUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public final class EigenvectorCentralityProc extends AlgoBaseProc<PageRank, PageRank, EigenvectorCentralityConfig> {
    public static final String DEFAULT_SCORE_PROPERTY = "eigenvector";
    //TODO
    static final String EIGENVECTOR_CENTRALITY_DESCRIPTION = "TODO";

    @Procedure(value = "gds.alpha.eigenvector.write", mode = Mode.WRITE)
    @Description(EIGENVECTOR_CENTRALITY_DESCRIPTION)
    public Stream<PageRankScore.Stats> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ComputationResult<PageRank, PageRank, EigenvectorCentralityConfig> result = compute(
            graphNameOrConfig,
            configuration
        );

        PageRankScore.Stats.Builder statsBuilder = new PageRankScore.Stats.Builder();
        CentralityResultWithStatistics stats = CentralityResultWithStatistics.of(result.result().result());
        String normalization = result.config().normalization();
        CentralityResult normalizedResults = normalization(normalization).apply(stats);

        PageRank prAlgo = result.algorithm();
        String writePropertyName = result.config().writeProperty();
        try (ProgressTimer ignored = statsBuilder.timeWrite()) {
            NodePropertyExporter exporter = NodePropertyExporter
                .of(api, result.graph(), TerminationFlag.wrap(transaction))
                .withLog(log)
                .parallel(Pools.DEFAULT, result.config().writeConcurrency())
                .build();
            normalizedResults.export(writePropertyName, exporter);
        }

        statsBuilder.withWrite(true).withWriteProperty(writePropertyName);

        statsBuilder
            .withIterations(prAlgo.iterations())
            .withDampingFactor(prAlgo.dampingFactor());

        return Stream.of(statsBuilder.build());
    }

    @Procedure(name = "gds.alpha.eigenvector.stream", mode = READ)
    @Description("CALL algo.eigenvector.stream(label:String, relationship:String, " +
                 "{weightProperty: null, concurrency:4}) " +
                 "YIELD node, score - calculates eigenvector centrality and streams results")
    public Stream<CentralityScore> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, EigenvectorCentralityConfig> result = compute(
            graphNameOrConfig,
            configuration
        );
        CentralityResultWithStatistics centralityResult = CentralityResultWithStatistics.of(result.result().result());
        String normalization = result.config().normalization();
        return CentralityUtils.streamResults(result.graph(), normalization(normalization).apply(centralityResult));
    }


    // TODO: extract the body into a Class
    @Override
    protected AlgorithmFactory<PageRank, EigenvectorCentralityConfig> algorithmFactory(EigenvectorCentralityConfig config) {
        return new AlgorithmFactory<PageRank, EigenvectorCentralityConfig>() {
            @Override
            public PageRank build(
                Graph graph,
                EigenvectorCentralityConfig configuration,
                AllocationTracker tracker,
                Log log
            ) {
                PageRank.Config algoConfig = new PageRank.Config(
                    configuration.maxIterations(),
                    1.0,
                    PageRank.DEFAULT_TOLERANCE
                );
                List<Node> sourceNodes = configuration.sourceNodes();
                LongStream sourceNodeIds = sourceNodes.stream().mapToLong(Node::getId);
                return LabsPageRankAlgorithmType.EIGENVECTOR_CENTRALITY
                    .create(
                        graph,
                        Pools.DEFAULT,
                        configuration.concurrency(),
                        ParallelUtil.DEFAULT_BATCH_SIZE,
                        algoConfig,
                        sourceNodeIds,
                        tracker
                    );
            }

            @Override
            public MemoryEstimation memoryEstimation(EigenvectorCentralityConfig configuration) {
                throw new UnsupportedOperationException("Estimation is not implemented for this algorithm.");
            }
        };
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

    public NormalizationFunction normalization(String normalization) {
        return NormalizationFunction.valueOf(normalization.toUpperCase());
    }
}
