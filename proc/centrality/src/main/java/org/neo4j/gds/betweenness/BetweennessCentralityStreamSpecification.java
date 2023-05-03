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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.core.utils.paged.HugeArrayToNodeProperties;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.betweenness.BetweennessCentrality.BETWEENNESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.betweenness.stream", description = BETWEENNESS_DESCRIPTION, executionMode = STREAM)
public class BetweennessCentralityStreamSpecification implements AlgorithmSpec<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityStreamConfig, Stream<CentralityStreamResult>, BetweennessCentralityFactory<BetweennessCentralityStreamConfig>> {

    @Override
    public String name() {
        return "BetweennessCentralityStream";
    }

    @Override
    public BetweennessCentralityFactory<BetweennessCentralityStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new BetweennessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<BetweennessCentralityStreamConfig> newConfigFunction() {
        return (__, userInput) -> BetweennessCentralityStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityStreamConfig, Stream<CentralityStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            ()  -> computationResult.result()
                .map(result -> {
                    var nodePropertyValues = HugeArrayToNodeProperties.convert(result);
                    var graph = computationResult.graph();
                    return LongStream
                        .range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(nodePropertyValues::hasValue)
                        .mapToObj(nodeId ->
                            new CentralityStreamResult(
                                graph.toOriginalNodeId(nodeId),
                                nodePropertyValues.doubleValue(nodeId)
                            ));
                }).orElseGet(Stream::empty)
        );
    }

    @Override
    public ValidationConfiguration<BetweennessCentralityStreamConfig> validationConfig(ExecutionContext executionContext) {
        return new BetweennessCentralityConfigValidation<>();
    }
}
