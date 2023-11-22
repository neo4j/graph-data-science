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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.procedures.centrality.celf.CELFWriteResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.influenceMaximization.CELFStreamProc.DESCRIPTION;

@GdsCallable(
    name = "gds.influenceMaximization.celf.write",
    aliases = {"gds.beta.influenceMaximization.celf.write"},
    description = DESCRIPTION,
    executionMode = WRITE_NODE_PROPERTY
)
public class CELFWriteSpec implements AlgorithmSpec<CELF, CELFResult, InfluenceMaximizationWriteConfig, Stream<CELFWriteResult>, CELFAlgorithmFactory<InfluenceMaximizationWriteConfig>> {
    @Override
    public String name() {
        return "CELFWrite";
    }

    @Override
    public CELFAlgorithmFactory<InfluenceMaximizationWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new CELFAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<InfluenceMaximizationWriteConfig> newConfigFunction() {
        return (__, userInput) -> InfluenceMaximizationWriteConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<CELF, CELFResult, InfluenceMaximizationWriteConfig, Stream<CELFWriteResult>> computationResultConsumer() {
        return new WriteNodePropertiesComputationResultConsumer<>(
            this::resultBuilder,
            computationResult -> List.of(NodeProperty.of(
                computationResult.config().writeProperty(),
                nodePropertyValues(computationResult)
            )),
            name()
        );
    }

    private NodePropertyValues nodePropertyValues(ComputationResult<CELF, CELFResult, InfluenceMaximizationWriteConfig> computationResult) {

        var celfResult = computationResult.result()
            .orElseGet(() -> new CELFResult(new LongDoubleScatterMap(0)));

        var nodeCount = computationResult.graph().nodeCount();

        return new CELFNodeProperties(celfResult.seedSetNodes(), nodeCount);

    }

    private AbstractResultBuilder<CELFWriteResult> resultBuilder(
        ComputationResult<CELF, CELFResult, InfluenceMaximizationWriteConfig> computationResult,
        ExecutionContext context
    ) {
        var celfResult = computationResult.result();

        var graph = computationResult.graph();
        return CELFWriteResult.builder()
            .withTotalSpread(celfResult.map(res -> res.totalSpread()).orElse(0D))
            .withNodeCount(graph.nodeCount())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config());
    }
}
