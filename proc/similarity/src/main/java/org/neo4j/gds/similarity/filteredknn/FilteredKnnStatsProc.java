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
package org.neo4j.gds.similarity.filteredknn;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.similarity.SimilarityGraphResult;
import org.neo4j.gds.similarity.SimilarityProc;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.similarity.SimilarityProc.computeHistogram;
import static org.neo4j.gds.similarity.SimilarityProc.shouldComputeHistogram;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.knn.filtered.stats", executionMode = STATS)
public final class FilteredKnnStatsProc extends StatsProc<FilteredKnn, FilteredKnnResult, FilteredKnnStatsResult, FilteredKnnStatsConfig> {
    @Procedure(name = "gds.alpha.knn.filtered.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<FilteredKnnStatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Override
    protected FilteredKnnStatsConfig newConfig(String username, CypherMapWrapper config) {
        return FilteredKnnStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<FilteredKnn, FilteredKnnStatsConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    protected AbstractResultBuilder<FilteredKnnStatsResult> resultBuilder(
        ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        throw new UnsupportedOperationException("Knn handles result building individually.");
    }

    @Override
    public Stream<FilteredKnnStatsResult> stats(ComputationResult<FilteredKnn, FilteredKnnResult, FilteredKnnStatsConfig> computationResult) {
        return runWithExceptionLogging("Graph stats failed", () -> {
            FilteredKnnStatsConfig config = computationResult.config();

            if (computationResult.isGraphEmpty()) {
                return Stream.of(
                    new FilteredKnnStatsResult(
                        computationResult.preProcessingMillis(),
                        0,
                        0,
                        0,
                        0,
                        Collections.emptyMap(),
                        true,
                        0,
                        0,
                        config.toMap()
                    )
                );
            }
            FilteredKnn algorithm = Objects.requireNonNull(computationResult.algorithm());
            FilteredKnnResult result = Objects.requireNonNull(computationResult.result());


            var resultBuilder = new FilteredKnnStatsResult.Builder()
                .withRanIterations(result.ranIterations())
                .withNodePairsConsidered(result.nodePairsConsidered())
                .withDidConverge(result.didConverge());

            SimilarityProc.resultBuilderWithTimings(resultBuilder, computationResult);

            if (shouldComputeHistogram(executionContext().returnColumns())) {
                try (ProgressTimer ignored = resultBuilder.timePostProcessing()) {
                    SimilarityGraphResult similarityGraphResult = FilteredKnnHelpers.computeToGraph(
                        computationResult.graph(),
                        algorithm.nodeCount(),
                        config.concurrency(),
                        result,
                        algorithm.executorService()
                    );

                    Graph similarityGraph = similarityGraphResult.similarityGraph();

                    resultBuilder
                        .withHistogram(computeHistogram(similarityGraph))
                        .withNodesCompared(similarityGraphResult.comparedNodes())
                        .withRelationshipsWritten(similarityGraph.relationshipCount());
                }
            } else {
                resultBuilder
                    .withNodesCompared(algorithm.nodeCount())
                    .withRelationshipsWritten(result.numberOfSimilarityPairs());
            }

            return Stream.of(resultBuilder.build());
        });
    }
}
