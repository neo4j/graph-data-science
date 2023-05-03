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
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutFactory;
import org.neo4j.gds.impl.approxmaxkcut.MaxKCutResult;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut.APPROX_MAX_K_CUT_DESCRIPTION;

@GdsCallable(name = "gds.alpha.maxkcut.stream", description = APPROX_MAX_K_CUT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ApproxMaxKCutStreamSpec implements AlgorithmSpec<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutStreamConfig, Stream<StreamResult>, ApproxMaxKCutFactory<ApproxMaxKCutStreamConfig>> {
    @Override
    public String name() {
        return "ApproxMaxKCutStream";
    }

    @Override
    public ApproxMaxKCutFactory<ApproxMaxKCutStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new ApproxMaxKCutFactory<>();
    }

    @Override
    public NewConfigFunction<ApproxMaxKCutStreamConfig> newConfigFunction() {
        return (___,config) -> ApproxMaxKCutStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) ->{
                if (computationResult.isGraphEmpty()){
                    return  Stream.empty();
                }
            var nodeProperties = CommunityProcCompanion.considerSizeFilter(
                computationResult.config(),
                computationResult.result()
                    .map(MaxKCutResult::asNodeProperties)
                    .orElse(EmptyLongNodePropertyValues.INSTANCE)
            );
                var graph = computationResult.graph();

                return LongStream.range(0,graph.nodeCount())
                    .filter(nodeProperties::hasValue)
                    .mapToObj( nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId),nodeProperties.longValue(nodeId)));
        };
    }
}
