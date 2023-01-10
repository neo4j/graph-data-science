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
package org.neo4j.gds.paths.topologicalsort;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.topologicalsort.TopologicalSort;
import org.neo4j.gds.topologicalsort.TopologicalSortConfig;
import org.neo4j.gds.topologicalsort.TopologicalSortConfigImpl;
import org.neo4j.gds.topologicalsort.TopologicalSortFactory;
import org.neo4j.gds.topologicalsort.TopologicalSortResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.topologicalSort.stream", description = TopologicalSortStreamProc.TOPOLOGICAL_SORT_DESCRIPTION, executionMode = STREAM)
public class TopologicalSortStreamProc extends StreamProc<TopologicalSort, TopologicalSortResult, TopologicalSortStreamProc.StreamResult, TopologicalSortConfig> {
    static final String TOPOLOGICAL_SORT_DESCRIPTION =
        "Returns all the nodes in the graph that are not part of a cycle or depend on a cycle, sorted in a topological order";

    @Procedure(value = "gds.alpha.topologicalSort.stream", mode = READ)
    @Description(TOPOLOGICAL_SORT_DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(compute(graphName, configuration));
    }

    @Override
    protected StreamResult streamResult(
        long originalNodeId,
        long internalNodeId,
        @Nullable NodePropertyValues nodePropertyValues
    ) {
        return new StreamResult(originalNodeId);
    }

    /**
     * Instead of mapping each node to its properties, we simply want to return the order of the nodes
     */
    @Override
    public ComputationResultConsumer<TopologicalSort, TopologicalSortResult, TopologicalSortConfig, Stream<StreamResult>> computationResultConsumer() {
        return (ComputationResult<TopologicalSort, TopologicalSortResult, TopologicalSortConfig> computationResult, ExecutionContext executionContext) ->
            runWithExceptionLogging("Result streaming failed", () -> {
                    if (computationResult.isGraphEmpty()) {
                        return Stream.empty();
                    }

                    Graph graph = computationResult.graph();
                    return LongStream.of(computationResult.result().value().toArray())
                        .mapToObj(nodeId -> streamResult(graph.toOriginalNodeId(nodeId), nodeId, null));
                }
            );
    }

    @Override
    protected TopologicalSortConfig newConfig(String username, CypherMapWrapper config) {
        return new TopologicalSortConfigImpl(config);
    }

    @Override
    public AlgorithmFactory<?, TopologicalSort, TopologicalSortConfig> algorithmFactory() {
        return new TopologicalSortFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StreamResult {

        public final long nodeId;

        public StreamResult(long nodeId) {
            this.nodeId = nodeId;
        }
    }
}
