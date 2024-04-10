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
package org.neo4j.gds.scaling;

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesMutateConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;
import static org.neo4j.gds.scaling.ScalePropertiesProc.validateLegacyScalers;

@GdsCallable(name = "gds.scaleProperties.mutate", aliases = {"gds.alpha.scaleProperties.mutate"}, description = SCALE_PROPERTIES_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ScalePropertiesMutateSpec implements AlgorithmSpec<ScaleProperties, ScalePropertiesResult, ScalePropertiesMutateConfig, Stream<ScalePropertiesMutateResult>, ScalePropertiesFactory<ScalePropertiesMutateConfig>> {

    private boolean allowL1L2Scalers = false;

    void setAllowL1L2Scalers(boolean allowL1L2Scalers) {
        this.allowL1L2Scalers = allowL1L2Scalers;
    }

    @Override
    public String name() {
        return "ScalePropertiesMutate";
    }

    @Override
    public ScalePropertiesFactory<ScalePropertiesMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ScalePropertiesFactory<>();
    }

    @Override
    public NewConfigFunction<ScalePropertiesMutateConfig> newConfigFunction() {
        return (__, userInput) -> {
            var config = ScalePropertiesMutateConfig.of(userInput);
            validateLegacyScalers(config, allowL1L2Scalers);
            return config;
        };
    }

    @Override
    public ComputationResultConsumer<ScaleProperties, ScalePropertiesResult, ScalePropertiesMutateConfig, Stream<ScalePropertiesMutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().mutateProperty(),
                ScalePropertiesProc.nodeProperties(computationResult)
            )),
            this::resultBuilder
        );
    }

    private AbstractResultBuilder<ScalePropertiesMutateResult> resultBuilder(
        ComputationResult<ScaleProperties, ScalePropertiesResult, ScalePropertiesMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new ScalePropertiesMutateResult.Builder();
        computationResult.result()
            .ifPresent(result -> builder.withScalerStatistics(result.scalerStatistics()));
        return builder;
    }
}
