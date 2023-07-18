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
package org.neo4j.gds.leiden;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.IdMap;
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
import static org.neo4j.gds.leiden.LeidenStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.beta.leiden.stream", description = DESCRIPTION, executionMode = STREAM)
public class LeidenStreamSpec implements AlgorithmSpec<Leiden, LeidenResult, LeidenStreamConfig, Stream<StreamResult>, LeidenAlgorithmFactory<LeidenStreamConfig>> {
    @Override
    public String name() {
        return "LeidenStream";
    }

    @Override
    public LeidenAlgorithmFactory<LeidenStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenStreamConfig> newConfigFunction() {
        return (__, config) -> LeidenStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, LeidenResult, LeidenStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var nodeProperties = CommunityProcCompanion.nodeProperties(
                        computationResult.config(),
                        LongNodePropertyValuesAdapter.create(result.dendrogramManager().getCurrent())
                    );
                    var includeIntermediateCommunities = computationResult.config().includeIntermediateCommunities();

                    return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(nodeProperties::hasValue)
                        .mapToObj(nodeId -> {
                            long[] intermediateCommunityIds = includeIntermediateCommunities
                                ? result.getIntermediateCommunities(nodeId)
                                : null;
                            long communityId = nodeProperties.longValue(nodeId);

                            return new StreamResult(
                                graph.toOriginalNodeId(nodeId),
                                intermediateCommunityIds,
                                communityId
                            );
                        });
                }).orElseGet(Stream::empty)
        );
    }
}
