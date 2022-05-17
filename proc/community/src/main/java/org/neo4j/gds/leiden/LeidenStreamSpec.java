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

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.leiden.LeidenStreamProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.leiden.stream", description = DESCRIPTION, executionMode = STREAM)
public class LeidenStreamSpec implements AlgorithmSpec<Leiden, HugeLongArray, LeidenStreamConfig, Stream<StreamResult>, LeidenAlgorithmFactory<LeidenStreamConfig>> {
    @Override
    public String name() {
        return "LeidenStream";
    }

    @Override
    public LeidenAlgorithmFactory<LeidenStreamConfig> algorithmFactory() {
        return new LeidenAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<LeidenStreamConfig> newConfigFunction() {
        return (__, config) -> LeidenStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Leiden, HugeLongArray, LeidenStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            var communities = computationResult.result();
            return LongStream.range(0, graph.nodeCount())
                .mapToObj(nodeId -> new StreamResult(graph.toOriginalNodeId(nodeId), communities.get(nodeId)));
        };
    }
}
