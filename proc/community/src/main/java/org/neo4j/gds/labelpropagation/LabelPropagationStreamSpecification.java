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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.labelpropagation.LabelPropagation.LABEL_PROPAGATION_DESCRIPTION;

@GdsCallable(name = "gds.labelPropagation.stream", description = LABEL_PROPAGATION_DESCRIPTION, executionMode = STREAM)
public class LabelPropagationStreamSpecification implements AlgorithmSpec<LabelPropagation, LabelPropagationResult, LabelPropagationStreamConfig, Stream<StreamResult>, LabelPropagationFactory<LabelPropagationStreamConfig>> {
    @Override
    public String name() {
        return "LabelPropagationStream";
    }

    @Override
    public LabelPropagationFactory<LabelPropagationStreamConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @Override
    public NewConfigFunction<LabelPropagationStreamConfig> newConfigFunction() {
        return (__, userInput) -> LabelPropagationStreamConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<LabelPropagation, LabelPropagationResult, LabelPropagationStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> {

                return Optional.ofNullable(computationResult.result())
                    .map(result -> {
                        var graph = computationResult.graph();
                        var nodePropertyValues = CommunityProcCompanion.nodeProperties(
                            computationResult.config(),
                            computationResult.result()
                                .map(LabelPropagationResult::labels)
                                .orElseGet(() -> HugeLongArray.newArray(0))
                                .asNodeProperties()
                        );
                        return LongStream
                            .range(IdMap.START_NODE_ID, graph.nodeCount())
                            .filter(nodePropertyValues::hasValue)
                            .mapToObj(nodeId -> new StreamResult(
                                graph.toOriginalNodeId(nodeId),
                                nodePropertyValues.longValue(nodeId)
                            ));
                    }).orElseGet(Stream::empty);
            }
        );
    }
}
