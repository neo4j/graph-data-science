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
package org.neo4j.gds.ml.splitting;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.alpha.ml.splitRelationships.mutate", description = "Splits a graph into holdout and remaining relationship types and adds them to the graph.", executionMode = MUTATE_RELATIONSHIP)
public class SplitRelationshipsMutateSpec implements AlgorithmSpec<SplitRelationships, EdgeSplitter.SplitResult,SplitRelationshipsMutateConfig, Stream<MutateResult>,SplitRelationshipsAlgorithmFactory> {

    @Override
    public String name() {
        return "SplitRelationships";
    }

    @Override
    public SplitRelationshipsAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        return new SplitRelationshipsAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<SplitRelationshipsMutateConfig> newConfigFunction() {
        return (___,config) -> SplitRelationshipsMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<SplitRelationships, EdgeSplitter.SplitResult, SplitRelationshipsMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<SplitRelationships, EdgeSplitter.SplitResult, SplitRelationshipsMutateConfig> computationResult,
                ExecutionContext executionContext
            ) {
                computationResult.result().ifPresent(splitResult -> {
                    var graphStore = computationResult.graphStore();
                    var selectedRels = splitResult.selectedRels().build();
                    var remainingRels = splitResult.remainingRels().build();

                    graphStore.addRelationshipType(remainingRels);
                    graphStore.addRelationshipType(selectedRels);

                    long holdoutWritten = selectedRels.topology().elementCount();
                    long remainingWritten = remainingRels.topology().elementCount();
                    resultBuilder.withRelationshipsWritten(holdoutWritten + remainingWritten);
                });
            }
        };
    }

    private  AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<SplitRelationships, EdgeSplitter.SplitResult, SplitRelationshipsMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }

}
