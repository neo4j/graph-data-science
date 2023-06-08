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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scc.Scc.NOT_VALID;
import static org.neo4j.gds.scc.Scc.SCC_DESCRIPTION;

@GdsCallable(name = "gds.alpha.scc.stream", description = SCC_DESCRIPTION, executionMode = STREAM)
public class SccStreamSpec implements AlgorithmSpec<Scc, HugeLongArray, SccStreamConfig, Stream<StreamResult>, SccAlgorithmFactory<SccStreamConfig>> {
    @Override
    public String name() {
        return "SccStream";
    }

    @Override
    public SccAlgorithmFactory<SccStreamConfig> algorithmFactory(ExecutionContext executionContext) {
        return new SccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SccStreamConfig> newConfigFunction() {
        return (__, config) -> SccStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Scc, HugeLongArray, SccStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Result streaming failed",
            executionContext.log(),
            () -> computationResult.result()
                .map(result -> {
                    var graph = computationResult.graph();
                    var components = computationResult.result().orElseGet(() -> HugeLongArray.newArray(0));
                    return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                        .filter(i -> components.get(i) != NOT_VALID)
                        .mapToObj(i -> new StreamResult(graph.toOriginalNodeId(i), components.get(i)));
                }).orElseGet(Stream::empty)
        );
    }
}
