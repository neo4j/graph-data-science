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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityWriteConsumer;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_RELATIONSHIP;
import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;
import static org.neo4j.gds.similarity.knn.KnnProc.computeToGraph;

@GdsCallable(name = "gds.knn.write", description = KNN_DESCRIPTION, executionMode = WRITE_RELATIONSHIP)
public class KnnWriteSpecification implements AlgorithmSpec<Knn, Knn.Result, KnnWriteConfig, Stream<WriteResult>, KnnFactory<KnnWriteConfig>> {
    @Override
    public String name() {
        return "KnnWrite";
    }

    @Override
    public KnnFactory<KnnWriteConfig> algorithmFactory() {
        return new KnnFactory<>();
    }

    @Override
    public NewConfigFunction<KnnWriteConfig> newConfigFunction() {
        return (__, userInput) -> KnnWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Knn, Knn.Result, KnnWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        return new SimilarityWriteConsumer<>(
            this::resultBuilderFunction,
            (computationResult) -> {
                return computeToGraph(
                    computationResult.graph(),
                    computationResult.algorithm().nodeCount(),
                    computationResult.config().concurrency(),
                    Objects.requireNonNull(computationResult.result()),
                    computationResult.algorithm().executorService()
                );
            },
            name()
        );
    }

    private WriteResult.Builder resultBuilderFunction(ComputationResult<Knn, Knn.Result, KnnWriteConfig> computationResult) {
        var builder = new WriteResult.Builder();

        Optional.ofNullable(computationResult.result()).ifPresent(result -> {
            builder
                .withDidConverge(result.didConverge())
                .withNodePairsConsidered(result.nodePairsConsidered())
                .withRanIterations(result.ranIterations());
        });

        return builder;
    }
}
