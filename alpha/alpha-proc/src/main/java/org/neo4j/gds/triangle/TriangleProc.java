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
package org.neo4j.gds.triangle;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.triangle.TriangleStream;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.triangle.TriangleProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.triangles", description = DESCRIPTION, executionMode = STREAM)
public class TriangleProc extends AlgoBaseProc<TriangleStream, Stream<TriangleStream.Result>, TriangleCountBaseConfig, TriangleStream.Result> {

    static final String DESCRIPTION = "Triangles streams the nodeIds of each triangle in the graph.";

    @Procedure(name = "gds.alpha.triangles", mode = READ)
    @Description(DESCRIPTION)
    public Stream<TriangleStream.Result> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration, false);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    protected TriangleCountBaseConfig newConfig(String username, CypherMapWrapper config) {
        return TriangleCountBaseConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<TriangleStream, TriangleCountBaseConfig> algorithmFactory() {
        return new GraphAlgorithmFactory<>() {
            @Override
            public String taskName() {
                return "TriangleStream";
            }

            @Override
            public TriangleStream build(
                Graph graph,
                TriangleCountBaseConfig configuration,
                ProgressTracker progressTracker
            ) {
                return TriangleStream.create(graph, Pools.DEFAULT, configuration.concurrency());
            }
        };
    }

    @Override
    public ComputationResultConsumer<TriangleStream, Stream<TriangleStream.Result>, TriangleCountBaseConfig, Stream<TriangleStream.Result>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            Graph graph = computationResult.graph();

            if (graph.isEmpty()) {
                return Stream.empty();
            }

            var resultStream = computationResult.result();
            try(var statement = transaction.acquireStatement()) {
                statement.registerCloseableResource(resultStream);
            }
            return resultStream;
        };
    }
}
