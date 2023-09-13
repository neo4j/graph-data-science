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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.approxmaxkcut.ApproxMaxKCut.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;

@GdsCallable(name = "gds.maxkcut.mutate", aliases = {"gds.alpha.maxkcut.mutate"}, description = APPROX_MAX_K_CUT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ApproxMaxKCutMutateSpec implements AlgorithmSpec<ApproxMaxKCut, ApproxMaxKCutResult, ApproxMaxKCutMutateConfig, Stream<MutateResult>, ApproxMaxKCutAlgorithmFactory<ApproxMaxKCutMutateConfig>> {
    @Override
    public String name() {
        return "ApproxMaxKCutMutate";
    }

    @Override
    public ApproxMaxKCutAlgorithmFactory<ApproxMaxKCutMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ApproxMaxKCutAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<ApproxMaxKCutMutateConfig> newConfigFunction() {
        return (___,config) -> ApproxMaxKCutMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<ApproxMaxKCut, ApproxMaxKCutResult, ApproxMaxKCutMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                CommunityProcCompanion.considerSizeFilter(
                    computationResult.config(),
                    computationResult.result()
                        .map(ApproxMaxKCutResult::candidateSolution)
                        .map(NodePropertyValuesAdapter::adapt)
                        .orElse(EmptyLongNodePropertyValues.INSTANCE)
                )
            )),
            this::resultBuilder
        );
    }
        private AbstractResultBuilder<MutateResult> resultBuilder(
            ComputationResult<ApproxMaxKCut, ApproxMaxKCutResult, ApproxMaxKCutMutateConfig> computationResult,
            ExecutionContext executionContext
     ) {
            return new MutateResult.Builder(
                computationResult.result()
                    .map(ApproxMaxKCutResult::cutCost)
                    .orElse(-1d)
            );
        }
}
