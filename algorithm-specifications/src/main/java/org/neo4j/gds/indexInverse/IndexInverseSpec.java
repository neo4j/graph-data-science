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
package org.neo4j.gds.indexInverse;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.miscellaneous.IndexInverseMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;
import java.util.stream.Stream;

@GdsCallable(name = IndexInverseSpec.CALLABLE_NAME, executionMode = ExecutionMode.MUTATE_RELATIONSHIP, description = Constants.INDEX_INVERSE_DESCRIPTION)
public class IndexInverseSpec implements AlgorithmSpec<InverseRelationships, Map<RelationshipType, SingleTypeRelationships>, InverseRelationshipsConfig, Stream<IndexInverseMutateResult>, InverseRelationshipsAlgorithmFactory> {

    static final String CALLABLE_NAME = "gds.graph.relationships.indexInverse";

    @Override
    public String name() {
        return CALLABLE_NAME;
    }

    @Override
    public InverseRelationshipsAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        return new InverseRelationshipsAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<InverseRelationshipsConfig> newConfigFunction() {
        return ((__, config) -> InverseRelationshipsConfig.of(config));
    }

    private AbstractResultBuilder<IndexInverseMutateResult> resultBuilder(
        ComputationResult<InverseRelationships, Map<RelationshipType, SingleTypeRelationships>, InverseRelationshipsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new IndexInverseMutateResult.Builder().withInputRelationships(computeResult.graph().relationshipCount());
    }

    @Override
    public ComputationResultConsumer<InverseRelationships, Map<RelationshipType, SingleTypeRelationships>, InverseRelationshipsConfig, Stream<IndexInverseMutateResult>> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<InverseRelationships, Map<RelationshipType, SingleTypeRelationships>, InverseRelationshipsConfig> computationResult,
                ExecutionContext executionContext
            ) {
                var graphStore = computationResult.graphStore();
                computationResult.result().ifPresent(result -> {
                    result.forEach((type, inverseRelationships) -> {
                        graphStore.addInverseIndex(type, inverseRelationships.topology(), inverseRelationships.properties());
                    });
                });
            }
        };
    }
}
