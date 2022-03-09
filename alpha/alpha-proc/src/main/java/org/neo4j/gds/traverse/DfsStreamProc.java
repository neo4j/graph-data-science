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
package org.neo4j.gds.traverse;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.traverse.DFS;
import org.neo4j.gds.impl.traverse.DfsStreamConfig;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.traverse.DfsStreamProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.dfs.stream", description = DESCRIPTION, executionMode = STREAM)
public class DfsStreamProc extends AlgoBaseProc<DFS, long[], DfsStreamConfig, DfsStreamProc.DfsStreamResult> {
    private static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    static final String DESCRIPTION =
        "DFS is a traversal algorithm, which explores all of the children nodes of " +
        "the present node prior to moving on to the next neighbour.";

    @Procedure(name = "gds.dfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<DfsStreamResult> dfs(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(name = "gds.dfs.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphName, configuration);
    }

    @Override
    public GraphAlgorithmFactory<DFS, DfsStreamConfig> algorithmFactory() {
        return new DFSAlgorithmFactory();
    }

    @Override
    protected DfsStreamConfig newConfig(String username, CypherMapWrapper config) {
        return DfsStreamConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<DFS, long[], DfsStreamConfig, Stream<DfsStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            long[] nodes = computationResult.result();
            if (graph.isEmpty() || null == nodes) {
                return Stream.empty();
            }

            var nodeList = Arrays.stream(nodes).boxed().map(graph::toOriginalNodeId).collect(Collectors.toList());
            var startNode = computationResult.config().sourceNode();
            return Stream.of(new DfsStreamResult(
                startNode,
                nodes,
                PathFactory.create(transaction.internalTransaction(), nodeList, NEXT)
            ));
        };
    }

    public static class DfsStreamResult {
        public Long sourceNode;
        public List<Long> nodeIds;
        public Path path;

        DfsStreamResult(long sourceNode, long[] nodes, Path path) {
            this.sourceNode = sourceNode;
            this.nodeIds = Arrays.stream(nodes)
                .boxed()
                .collect(Collectors.toList());
            this.path = path;
        }
    }
}
