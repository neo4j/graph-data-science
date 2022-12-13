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
package org.neo4j.gds.beta.undirected;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardMutateResult;

import java.util.Map;
import java.util.stream.Stream;

@GdsCallable(name = "gds.beta.graph.relationships.toUndirected", executionMode = ExecutionMode.MUTATE_RELATIONSHIP, description = ToUndirectedSpec.DESCRIPTION)
public class ToUndirectedSpec implements AlgorithmSpec<ToUndirected, SingleTypeRelationshipImportResult, ToUndirectedConfig, Stream<ToUndirectedSpec.MutateResult>, ToUndirectedFactory> {

    public static final String DESCRIPTION = "The ToUndirected procedure converts directed relationships to undirected relationships";

    @Override
    public String name() {
        return "gds.beta.graph.relationships.toUndirected";
    }

    @Override
    public ToUndirectedFactory algorithmFactory() {
        return new ToUndirectedFactory();
    }

    @Override
    public NewConfigFunction<ToUndirectedConfig> newConfigFunction() {
        return ((__, config) -> ToUndirectedConfig.of(config));
    }

    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ToUndirected, SingleTypeRelationshipImportResult, ToUndirectedConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder().withInputRelationships(computeResult.graph().relationshipCount());
    }

    @Override
    public ComputationResultConsumer<ToUndirected, SingleTypeRelationshipImportResult, ToUndirectedConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutateComputationResultConsumer<>(this::resultBuilder) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<ToUndirected, SingleTypeRelationshipImportResult, ToUndirectedConfig> computationResult,
                ExecutionContext executionContext
            ) {
                computationResult.graphStore().addRelationshipType(
                    RelationshipType.of(computationResult.config().mutateRelationshipType()),
                    computationResult.result()
                );
            }
        };
    }

    public static final class MutateResult extends StandardMutateResult {
        public final long inputRelationships;
        public final long relationshipsWritten;

        private MutateResult(
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long inputRelationships,
            long relationshipsWritten,
            Map<String, Object> configuration
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, mutateMillis, configuration);
            this.inputRelationships = inputRelationships;
            this.relationshipsWritten = relationshipsWritten;
        }

        public static class Builder extends AbstractResultBuilder<MutateResult> {

            private long inputRelationships;

            Builder withInputRelationships(long inputRelationships) {
                this.inputRelationships = inputRelationships;
                return this;
            }

            @Override
            public MutateResult build() {
                return new MutateResult(
                    preProcessingMillis,
                    computeMillis,
                    0L,
                    mutateMillis,
                    inputRelationships,
                    relationshipsWritten,
                    config.toMap()
                );
            }
        }
    }
}
