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
package org.neo4j.gds.undirected;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.miscellaneous.ToUndirectedMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

@GdsCallable(
    name = ToUndirectedSpec.CALLABLE_NAME,
    executionMode = ExecutionMode.MUTATE_RELATIONSHIP,
    description = Constants.TO_UNDIRECTED_DESCRIPTION,
    aliases = {"gds.beta.graph.relationships.toUndirected"}
)
public class ToUndirectedSpec implements AlgorithmSpec<ToUndirected, SingleTypeRelationships, ToUndirectedConfig, Stream<ToUndirectedMutateResult>, ToUndirectedAlgorithmFactory> {

    static final String CALLABLE_NAME = "gds.graph.relationships.toUndirected";

    @Override
    public String name() {
        return CALLABLE_NAME;
    }

    @Override
    public ToUndirectedAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        return new ToUndirectedAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<ToUndirectedConfig> newConfigFunction() {
        return ((__, config) -> ToUndirectedConfig.of(config));
    }

    protected AbstractResultBuilder<ToUndirectedMutateResult> resultBuilder(
        ComputationResult<ToUndirected, SingleTypeRelationships, ToUndirectedConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new ToUndirectedMutateResult.Builder().withInputRelationships(computeResult.graph().relationshipCount());
    }

    @Override
    public ComputationResultConsumer<ToUndirected, SingleTypeRelationships, ToUndirectedConfig, Stream<ToUndirectedMutateResult>> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<ToUndirected, SingleTypeRelationships, ToUndirectedConfig> computationResult,
                ExecutionContext executionContext
            ) {
                computationResult.result().ifPresent(result -> {
                    computationResult.graphStore().addRelationshipType(result);
                    resultBuilder.withRelationshipsWritten(result.topology().elementCount());
                });
            }
        };
    }
}
