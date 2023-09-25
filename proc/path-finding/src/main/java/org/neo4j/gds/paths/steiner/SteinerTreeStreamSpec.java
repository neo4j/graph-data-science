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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeAlgorithmFactory;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;

@GdsCallable(
    name = "gds.steinerTree.stream",
    aliases = {"gds.beta.steinerTree.stream"},
    description = Constants.DESCRIPTION,
    executionMode = STREAM
)
public class SteinerTreeStreamSpec implements AlgorithmSpec<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStreamConfig, Stream<StreamResult>, SteinerTreeAlgorithmFactory<SteinerTreeStreamConfig>> {

    @Override
    public String name() {
        return "SteinerTreeStream";
    }

    @Override
    public SteinerTreeAlgorithmFactory<SteinerTreeStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SteinerTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SteinerTreeStreamConfig> newConfigFunction() {
        return (__, config) -> SteinerTreeStreamConfig.of(config);

    }

    public ComputationResultConsumer<ShortestPathsSteinerAlgorithm, SteinerTreeResult, SteinerTreeStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var sourceNode = computationResult.config().sourceNode();
                    var graph = computationResult.graph();
                    var parents = result.parentArray();
                    var costs = result.relationshipToParentCost();
                    return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(nodeId -> parents.get(nodeId) != ShortestPathsSteinerAlgorithm.PRUNED)
                        .mapToObj(nodeId -> {
                            var originalId = graph.toOriginalNodeId(nodeId);
                            return new StreamResult(
                                originalId,
                                (sourceNode == originalId) ? sourceNode : graph.toOriginalNodeId(parents.get(nodeId)),
                                costs.get(nodeId)
                            );
                        });
                }).orElseGet(Stream::empty)
        );
    }
}
