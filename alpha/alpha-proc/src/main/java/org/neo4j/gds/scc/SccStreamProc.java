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
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.scc.SccAlgorithm;
import org.neo4j.gds.impl.scc.SccConfig;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.scc.SccProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.scc.stream", description = DESCRIPTION, executionMode = STREAM)
public class SccStreamProc extends SccProc<SccAlgorithm.StreamResult> {

    @Procedure(value = "gds.alpha.scc.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SccAlgorithm.StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());

    }

    @Override
    public ComputationResultConsumer<SccAlgorithm, HugeLongArray, SccConfig, Stream<SccAlgorithm.StreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();
            HugeLongArray components = computationResult.result();

            if (graph.isEmpty()) {
                return Stream.empty();
            }

            return LongStream.range(0, graph.nodeCount())
                .filter(i -> components.get(i) != -1)
                .mapToObj(i -> new SccAlgorithm.StreamResult(graph.toOriginalNodeId(i), components.get(i)));
        };
    }
}
