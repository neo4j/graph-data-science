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
import org.neo4j.gds.impl.traverse.BFS;
import org.neo4j.gds.impl.traverse.BfsConfig;
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
import static org.neo4j.gds.traverse.BfsProc.DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.bfs.stream", description = DESCRIPTION, executionMode = STREAM)
public class BfsProc extends AlgoBaseProc<BFS, long[], BfsConfig, BfsProc.BfsResult> {
    private static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    static final String DESCRIPTION =
        "BFS is a traversal algorithm, which explores all of the neighbor nodes at " +
        "the present depth prior to moving on to the nodes at the next depth level.";

    @Procedure(name = "gds.bfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<BfsResult> bfs(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var computationResult = compute(graphName, configuration);
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Procedure(name = "gds.bfs.stream.estimate", mode = READ)
    @Description(DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphName, configuration);
    }

    @Override
    public GraphAlgorithmFactory<BFS, BfsConfig> algorithmFactory() {
        return new BFSAlgorithmFactory();
    }

    @Override
    protected BfsConfig newConfig(String username, CypherMapWrapper config) {
        return BfsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<BFS, long[], BfsConfig, Stream<BfsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var graph = computationResult.graph();
            if (graph.isEmpty()) {
                return Stream.empty();
            }

            long[] nodes = computationResult.result();
            var nodeList = Arrays.stream(nodes).boxed().map(graph::toOriginalNodeId).collect(Collectors.toList());
            var startNode = computationResult.config().sourceNode();
            return Stream.of(new BfsResult(
                startNode,
                nodes,
                PathFactory.create(transaction.internalTransaction(), nodeList, NEXT)
            ));
        };
    }

    public static class BfsResult {
        public Long sourceNode;
        public List<Long> nodeIds;
        public Path path;

        public BfsResult(long sourceNode, long[] nodes, Path path) {
            this.sourceNode = sourceNode;
            this.nodeIds = Arrays.stream(nodes)
                .boxed()
                .collect(Collectors.toList());
            this.path = path;
        }
    }
}
