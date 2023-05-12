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
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.stream.Stream;

import static org.neo4j.gds.similarity.knn.KnnProc.KNN_DESCRIPTION;

@GdsCallable(name = "gds.knn.stream", description = KNN_DESCRIPTION, executionMode = ExecutionMode.STREAM)
public class KnnStreamSpecification implements AlgorithmSpec<Knn, Knn.Result, KnnStreamConfig, Stream<SimilarityResult>, KnnFactory<KnnStreamConfig>> {

    @Override
    public String name() {
        return "KnnStream";
    }

    @Override
    public KnnFactory<KnnStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KnnFactory<>();
    }

    @Override
    public NewConfigFunction<KnnStreamConfig> newConfigFunction() {
        return (__, userInput) -> KnnStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Knn, Knn.Result, KnnStreamConfig, Stream<SimilarityResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            return computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    return result.streamSimilarityResult()
                        .map(similarityResult -> {
                            similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                            similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                            return similarityResult;
                        });

                }).orElseGet(Stream::empty);
        };
    }
}
