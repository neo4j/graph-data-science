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
package org.neo4j.gds.scc;

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.community.SccMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.scc.Scc.SCC_DESCRIPTION;

@GdsCallable(
    name = "gds.scc.mutate",
    description = SCC_DESCRIPTION,
    executionMode = ExecutionMode.MUTATE_NODE_PROPERTY
)
public class SccMutateSpec implements AlgorithmSpec<Scc, HugeLongArray, SccMutateConfig, Stream<SccMutateResult>, SccAlgorithmFactory<SccMutateConfig>> {

    @Override
    public String name() {
        return "SccMutate";
    }

    @Override
    public SccAlgorithmFactory<SccMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SccMutateConfig> newConfigFunction() {
        return (__, config) -> SccMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Scc, HugeLongArray, SccMutateConfig, Stream<SccMutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                computationResult.result()
                    .map(NodePropertyValuesAdapter::adapt)
                    .orElse(EmptyLongNodePropertyValues.INSTANCE)
            )),
            this::resultBuilder);
    }

    private AbstractResultBuilder<SccMutateResult> resultBuilder(
        ComputationResult<Scc, HugeLongArray, SccMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var config = computationResult.config();
        var mutateBuilder = new SccMutateResult.Builder(
            executionContext.returnColumns(),
            config.concurrency()
        )
            .buildCommunityCount(true)
            .buildHistogram(true);

        computationResult.result().ifPresent(result -> mutateBuilder.withCommunityFunction(result::get));

        mutateBuilder
            .withNodeCount(computationResult.graph().nodeCount())
            .withConfig(config)
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis());

        return mutateBuilder;
    }
}
