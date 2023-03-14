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
package org.neo4j.gds.scc;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.scc.Scc;
import org.neo4j.gds.impl.scc.SccAlgorithmFactory;
import org.neo4j.gds.impl.scc.SccStreamConfig;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scc.SccWriteProc.DESCRIPTION;

@GdsCallable(name = "gds.alpha.scc.stream", description = DESCRIPTION, executionMode = STREAM)
public class SccStreamSpec implements AlgorithmSpec<Scc, HugeLongArray, SccStreamConfig, Stream<StreamResult>, SccAlgorithmFactory<SccStreamConfig>> {
    @Override
    public String name() {
        return "SccStream";
    }

    @Override
    public SccAlgorithmFactory<SccStreamConfig> algorithmFactory() {
        return new SccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SccStreamConfig> newConfigFunction() {
        return (__, config) -> SccStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Scc, HugeLongArray, SccStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            HugeLongArray components = computationResult.result();

            if (graph.isEmpty()) {
                return Stream.empty();
            }

            return LongStream.range(0, graph.nodeCount())
                .filter(i -> components.get(i) != -1)
                .mapToObj(i -> new StreamResult(graph.toOriginalNodeId(i), components.get(i)));
        };
    }
}
