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
package org.neo4j.gds.walking;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.beta.walking.CollapsePath;
import org.neo4j.gds.beta.walking.CollapsePathAlgorithmFactory;
import org.neo4j.gds.beta.walking.CollapsePathConfig;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;
import static org.neo4j.gds.walking.CollapsePathMutateProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.collapsePath.mutate", description = DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class CollapsePathMutateSpec implements AlgorithmSpec<CollapsePath, SingleTypeRelationships, CollapsePathConfig,Stream<MutateResult>, CollapsePathAlgorithmFactory> {

    @Override
    public String name() {
        return "CollapsePath";
    }

    @Override
    public CollapsePathAlgorithmFactory algorithmFactory() {
        return new CollapsePathAlgorithmFactory();
    }

    @Override
    public NewConfigFunction<CollapsePathConfig> newConfigFunction() {
        return (username, config) -> CollapsePathConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<CollapsePath, SingleTypeRelationships, CollapsePathConfig, Stream<MutateResult>> computationResultConsumer() {
            return new MutateComputationResultConsumer<>(this::resultBuilder) {
                @Override
                protected void updateGraphStore(
                    AbstractResultBuilder<?> resultBuilder,
                    ComputationResult<CollapsePath, SingleTypeRelationships, CollapsePathConfig> computationResult,
                    ExecutionContext executionContext
                ) {
                    computationResult.result().ifPresent(result -> {
                        computationResult.graphStore().addRelationshipType(result);
                        resultBuilder.withRelationshipsWritten(result.topology().elementCount());
                    });
                }
            };
        }


    private AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<CollapsePath, SingleTypeRelationships, CollapsePathConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder();
    }

}
