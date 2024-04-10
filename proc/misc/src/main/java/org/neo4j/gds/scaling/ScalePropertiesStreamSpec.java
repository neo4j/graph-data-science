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

import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.misc.scaleproperties.ScalePropertiesStreamResult;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesFactory;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.scaleproperties.ScalePropertiesStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scaling.ScalePropertiesProc.SCALE_PROPERTIES_DESCRIPTION;
import static org.neo4j.gds.scaling.ScalePropertiesProc.validateLegacyScalers;

@GdsCallable(name = "gds.scaleProperties.stream", aliases = {"gds.alpha.scaleProperties.stream"}, description = SCALE_PROPERTIES_DESCRIPTION, executionMode = STREAM)
public class ScalePropertiesStreamSpec implements AlgorithmSpec<ScaleProperties, ScalePropertiesResult, ScalePropertiesStreamConfig, Stream<ScalePropertiesStreamResult>, ScalePropertiesFactory<ScalePropertiesStreamConfig>> {

    private boolean allowL1L2Scalers = false;

    void setAllowL1L2Scalers(boolean allowL1L2Scalers) {
        this.allowL1L2Scalers = allowL1L2Scalers;
    }

    @Override
    public String name() {
        return "ScalePropertiesStream";
    }

    @Override
    public ScalePropertiesFactory<ScalePropertiesStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ScalePropertiesFactory<>();
    }

    @Override
    public NewConfigFunction<ScalePropertiesStreamConfig> newConfigFunction() {
        return (__, userInput) -> {
            var config = ScalePropertiesStreamConfig.of(userInput);
            validateLegacyScalers(config, allowL1L2Scalers);
            return config;
        };
    }

    @Override
    public ComputationResultConsumer<ScaleProperties, ScalePropertiesResult, ScalePropertiesStreamConfig, Stream<ScalePropertiesStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var nodeProperties = ScalePropertiesProc.nodeProperties(graph.nodeCount(), result.scaledProperties());
                    return LongStream
                        .range(0, graph.nodeCount())
                        .mapToObj(nodeId -> new ScalePropertiesStreamResult(
                            graph.toOriginalNodeId(nodeId),
                            nodeProperties.doubleArrayValue(nodeId)
                        ));
                }).orElseGet(Stream::empty)
        );
    }
}
