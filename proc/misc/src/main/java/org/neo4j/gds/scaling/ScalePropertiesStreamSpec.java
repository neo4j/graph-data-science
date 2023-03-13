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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scaleProperties.stream", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = STREAM)
@GdsCallable(name = "gds.beta.scaleProperties.stream", description = SCALE_PROPERTIES_DESCRIPTION, executionMode = STREAM)
public class ScalePropertiesStreamSpec implements AlgorithmSpec<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamConfig, Stream<ScalePropertiesStreamProc.Result>, ScalePropertiesFactory<ScalePropertiesStreamConfig>> {

    @Override
    public String name() {
        return "ScalePropertiesStream";
    }

    @Override
    public ScalePropertiesFactory<ScalePropertiesStreamConfig> algorithmFactory() {
        return new ScalePropertiesFactory<>();
    }

    @Override
    public NewConfigFunction<ScalePropertiesStreamConfig> newConfigFunction() {
        return (__, userInput) -> ScalePropertiesStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<ScaleProperties, ScaleProperties.Result, ScalePropertiesStreamConfig, Stream<ScalePropertiesStreamProc.Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var result = computationResult.result();
            if (result == null) {
                return Stream.empty();
            }
            var graph = computationResult.graph();

            NodePropertyValues nodeProperties = ScalePropertiesProc.nodeProperties(computationResult);

            return LongStream
                .range(0, graph.nodeCount())
                .mapToObj(nodeId -> new ScalePropertiesStreamProc.Result(
                    graph.toOriginalNodeId(nodeId),
                    nodeProperties.doubleArrayValue(nodeId)
                ));
        };
    }
}
