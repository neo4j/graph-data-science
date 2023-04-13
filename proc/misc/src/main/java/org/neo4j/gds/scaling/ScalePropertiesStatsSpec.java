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

import org.neo4j.gds.StatsComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesStatsConfig;
import org.neo4j.gds.scaling.ScalePropertiesStatsProc.StatsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;
import static org.neo4j.gds.scaling.ScalePropertiesProc.validateLegacyScalers;

@GdsCallable(name = "gds.beta.scaleProperties.stats", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = STREAM)
public class ScalePropertiesStatsSpec implements AlgorithmSpec<ScaleProperties, ScaleProperties.Result, ScalePropertiesStatsConfig, Stream<StatsResult>, ScalePropertiesFactory<ScalePropertiesStatsConfig>> {
    @Override
    public String name() {
        return "ScalePropertiesStats";
    }

    @Override
    public ScalePropertiesFactory<ScalePropertiesStatsConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    public NewConfigFunction<ScalePropertiesStatsConfig> newConfigFunction() {
        return (__, userInput) -> {
            var config = ScalePropertiesStatsConfig.of(userInput);
            validateLegacyScalers(config, false);
            return config;
        };
    }

    @Override
    public ComputationResultConsumer<ScaleProperties, ScaleProperties.Result, ScalePropertiesStatsConfig, Stream<ScalePropertiesStatsProc.StatsResult>> computationResultConsumer() {
        return new StatsComputationResultConsumer<>(this::resultBuilder);
    }

    private AbstractResultBuilder<ScalePropertiesStatsProc.StatsResult> resultBuilder(
        ComputationResult<ScaleProperties, ScaleProperties.Result, ScalePropertiesStatsConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var builder = new StatsResult.Builder();

        computationResult.result().ifPresent(result -> builder.withScalerStatistics(result.scalerStatistics()));

        return builder;
    }


}
