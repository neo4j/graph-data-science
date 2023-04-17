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
package org.neo4j.gds.paths.steiner;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(name = "gds.beta.SteinerTree.stream", description = SteinerTreeStatsProc.DESCRIPTION, executionMode = STREAM)
public class SteinerTreeStreamSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStreamConfig, Stream<StreamResult>, SteinerTreeAlgorithmFactory<SteinerTreeStreamConfig>> {

    @Override
    public String name() {
        return "SteinerTreeStream";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeStreamConfig> algorithmFactory() {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeStreamConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeStreamConfig.of(config);

    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStreamConfig, Stream<StreamResult>> computationResultConsumer() {

        return (computationResult, executionContext) -> {
            if (computationResult.result().isEmpty()) {
                return Stream.empty();
            }

            var sourceNode = computationResult.config().sourceNode();
            Graph graph = computationResult.graph();
            var steinerTreeResult = computationResult.result().get();
            var parentArray = steinerTreeResult.parentArray();
            var costArray = steinerTreeResult.relationshipToParentCost();
            return LongStream.range(0, graph.nodeCount())
                .filter(nodeId -> parentArray.get(nodeId) != ShortestPathsSteinerAlgorithm.PRUNED)
                .mapToObj(nodeId -> new StreamResult(
                    graph.toOriginalNodeId(nodeId),
                    (sourceNode == graph.toOriginalNodeId(nodeId)) ?
                        sourceNode :
                        graph.toOriginalNodeId(parentArray.get(nodeId)),
                    costArray.get(nodeId)
                ));
        };
    }
}
