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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Optional;
import java.util.stream.Stream;

@GdsCallable(name = "gds.alpha.knn.filtered.stream", executionMode = ExecutionMode.STREAM)
public class FilteredKnnStreamSpecification implements AlgorithmSpec<FilteredKnn, FilteredKnnResult, FilteredKnnStreamConfig, Stream<SimilarityResult>, FilteredKnnFactory<FilteredKnnStreamConfig>> {

    @Override
    public String name() {
        return "FilteredKnnStream";
    }

    @Override
    public FilteredKnnFactory<FilteredKnnStreamConfig> algorithmFactory() {
        return new FilteredKnnFactory<>();
    }

    @Override
    public NewConfigFunction<FilteredKnnStreamConfig> newConfigFunction() {
        return (__, userInput) -> FilteredKnnStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<FilteredKnn, FilteredKnnResult, FilteredKnnStreamConfig, Stream<SimilarityResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            return Optional.ofNullable(computationResult.result())
                .map(result -> {
                    var graph = computationResult.graph();
                    return result.similarityResultStream()
                        .map(similarityResult -> {
                            similarityResult.node1 = graph.toOriginalNodeId(similarityResult.node1);
                            similarityResult.node2 = graph.toOriginalNodeId(similarityResult.node2);
                            return similarityResult;
                        });

                }).orElseGet(Stream::empty);
        };
    }
}
