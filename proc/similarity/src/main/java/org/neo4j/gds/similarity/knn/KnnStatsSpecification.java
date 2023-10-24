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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.procedures.similarity.knn.KnnStatsResult;
import org.neo4j.gds.similarity.SimilarityProc;

import java.util.Objects;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;

@GdsCallable(name = "gds.knn.stats", description = KNN_DESCRIPTION, executionMode = STATS)
public class KnnStatsSpecification implements AlgorithmSpec<Knn, KnnResult, KnnStatsConfig, Stream<KnnStatsResult>, KnnFactory<KnnStatsConfig>> {
    @Override
    public String name() {
        return "KnnStats";
    }

    @Override
    public KnnFactory<KnnStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KnnFactory<>();
    }

    @Override
    public NewConfigFunction<KnnStatsConfig> newConfigFunction() {
        return (__, userInput) -> KnnStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Knn, KnnResult, KnnStatsConfig, Stream<KnnStatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {

            var config = computationResult.config();
            var builder = new KnnStatsResultBuilder();

            computationResult.result().ifPresent(result -> {
                builder
                    .withRanIterations(result.ranIterations())
                    .withNodePairsConsidered(result.nodePairsConsidered())
                    .withDidConverge(result.didConverge());

                // algorithm is only null if the result is null
                var knn = Objects.requireNonNull(computationResult.algorithm());

                if (SimilarityProc.shouldComputeHistogram(executionContext.returnColumns())) {
                    try (ProgressTimer ignored = builder.timePostProcessing()) {
                        var similarityGraphResult = KnnProc.computeToGraph(
                            computationResult.graph(),
                            computationResult.graph().nodeCount(),
                            config.concurrency(),
                            result,
                            knn.executorService()
                        );

                        var similarityGraph = similarityGraphResult.similarityGraph();

                        builder
                            .withHistogram(SimilarityProc.computeHistogram(similarityGraph))
                            .withNodesCompared(similarityGraphResult.comparedNodes())
                            .withRelationshipsWritten(similarityGraph.relationshipCount());
                    }
                } else {
                    builder
                        .withNodesCompared(computationResult.graph().nodeCount())
                        .withRelationshipsWritten(result.totalSimilarityPairs());
                }
            });


            return Stream.of(
                builder
                    .withPreProcessingMillis(computationResult.preProcessingMillis())
                    .withComputeMillis(computationResult.computeMillis())
                    .withConfig(config)
                    .build()
            );
        };
    }
}
