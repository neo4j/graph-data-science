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
package org.neo4j.gds.louvain;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.nodeproperties.LongNodePropertyValuesAdapter;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.louvain.LouvainConstants.DESCRIPTION;

@GdsCallable(name = "gds.louvain.stream", description = DESCRIPTION, executionMode = STREAM)
public class LouvainStreamSpec implements AlgorithmSpec<Louvain, LouvainResult, LouvainStreamConfig, Stream<LouvainStreamResult>, LouvainAlgorithmFactory<LouvainStreamConfig>> {
    @Override
    public String name() {
        return "LouvainStream";
    }

    @Override
    public LouvainAlgorithmFactory<LouvainStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LouvainAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LouvainStreamConfig> newConfigFunction() {
        return (__, config) -> LouvainStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Louvain, LouvainResult, LouvainStreamConfig, Stream<LouvainStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var config = computationResult.config();
                    var nodePropertyValues = CommunityProcCompanion.nodeProperties(
                        config,
                        LongNodePropertyValuesAdapter.create(result.dendrogramManager().getCurrent())
                    );
                    var includeIntermediateCommunities = config.includeIntermediateCommunities();

                    return LongStream.range(0, graph.nodeCount())
                        .boxed().
                        filter(nodePropertyValues::hasValue)
                        .map(nodeId -> {
                            var communities = includeIntermediateCommunities
                                ? result.getIntermediateCommunities(nodeId)
                                : null;
                            var communityId = nodePropertyValues.longValue(nodeId);
                            return new LouvainStreamResult(graph.toOriginalNodeId(nodeId), communities, communityId);
                        });
                }).orElseGet(Stream::empty)
        );
    }
}
