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

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;

@GdsCallable(name = "gds.beta.scaleProperties.write", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = ExecutionMode.WRITE_NODE_PROPERTY)
public class ScalePropertiesWriteSpec implements AlgorithmSpec<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig, Stream<ScalePropertiesWriteProc.WriteResult>, ScalePropertiesFactory<ScalePropertiesWriteConfig>> {

    @Override
    public String name() {
        return "ScalePropertiesWrite";
    }

    @Override
    public ScalePropertiesFactory<ScalePropertiesWriteConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    public NewConfigFunction<ScalePropertiesWriteConfig> newConfigFunction() {
        return (__, userInput) -> ScalePropertiesWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ScaleProperties, ScaleProperties.Result, ScalePropertiesWriteConfig, Stream<ScalePropertiesWriteProc.WriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            (computationResult, __) ->
                new ScalePropertiesWriteProc.WriteResult.Builder().withScalerStatistics(
                    computationResult.result().scalerStatistics()
                ),
            computationResult -> List.of(ImmutableNodeProperty.of(
                computationResult.config().writeProperty(),
                ScalePropertiesProc.nodeProperties(computationResult)
            )),
            name()
        );
    }
}
