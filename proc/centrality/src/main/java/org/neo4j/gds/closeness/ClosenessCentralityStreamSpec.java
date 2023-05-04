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
package org.neo4j.gds.closeness;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.beta.closeness.ClosenessCentrality;
import org.neo4j.gds.beta.closeness.ClosenessCentralityFactory;
import org.neo4j.gds.beta.closeness.ClosenessCentralityResult;
import org.neo4j.gds.beta.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.closeness.ClosenessCentrality.CLOSENESS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.beta.closeness.stream", description = CLOSENESS_DESCRIPTION, executionMode = STREAM)
public class ClosenessCentralityStreamSpec implements AlgorithmSpec<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStreamConfig, Stream<CentralityStreamResult>, ClosenessCentralityFactory<ClosenessCentralityStreamConfig>> {

    @Override
    public String name() {
        return "ClosenessCentralityStream";
    }

    @Override
    public ClosenessCentralityFactory<ClosenessCentralityStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ClosenessCentralityFactory<>();
    }

    @Override
    public NewConfigFunction<ClosenessCentralityStreamConfig> newConfigFunction() {
        return (___, config) -> ClosenessCentralityStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStreamConfig, Stream<CentralityStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {

            if (computationResult.isGraphEmpty()) {
                return Stream.empty();
            }

            var nodePropertyValues = computationResult.result()
                .map(ClosenessCentralityResult::centralities)
                .orElseGet(() -> HugeDoubleArray.newArray(0))
                .asNodeProperties();
            var graph = computationResult.graph();
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId ->
                    new CentralityStreamResult(
                        graph.toOriginalNodeId(nodeId),
                        nodePropertyValues.doubleValue(nodeId)
                    ));

        };
    }
}
