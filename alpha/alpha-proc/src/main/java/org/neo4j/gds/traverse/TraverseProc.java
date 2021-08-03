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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.AlphaAlgorithmFactory;
import org.neo4j.gds.impl.traverse.Traverse;
import org.neo4j.gds.impl.traverse.TraverseConfig;
import org.neo4j.gds.impl.walking.WalkPath;
import org.neo4j.gds.impl.walking.WalkResult;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.InputNodeValidator.validateEndNode;
import static org.neo4j.gds.utils.InputNodeValidator.validateStartNode;
import static org.neo4j.procedure.Mode.READ;

public class TraverseProc extends AlgoBaseProc<Traverse, Traverse, TraverseConfig> {

    private static final String DESCRIPTION =
        "BFS is a traversal algorithm, which explores all of the neighbor nodes at " +
        "the present depth prior to moving on to the nodes at the next depth level.";
    private static boolean isBfs;

    @Procedure(name = "gds.alpha.bfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<WalkResult> bfs(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        isBfs = true;
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.dfs.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<WalkResult> dfs(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        isBfs = false;
        return stream(graphNameOrConfig, configuration);
    }

    @Override
    protected TraverseConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return TraverseConfig.of(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    protected AlgorithmFactory<Traverse, TraverseConfig> algorithmFactory() {
        return (AlphaAlgorithmFactory<Traverse, TraverseConfig>) (graph, configuration, tracker, log, eventTracker) -> {
            Traverse.ExitPredicate exitFunction;
            Traverse.Aggregator aggregatorFunction;
            // target node given; terminate if target is reached
            if (!configuration.targetNodes().isEmpty()) {
                List<Long> mappedTargets = configuration.targetNodes().stream()
                    .map(graph::toMappedNodeId)
                    .collect(Collectors.toList());
                exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
                aggregatorFunction = (s, t, w) -> .0;
                // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
            } else if (configuration.maxDepth() != -1) {
                exitFunction = (s, t, w) -> w > configuration.maxDepth() ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
                aggregatorFunction = (s, t, w) -> w + 1.;
                // do complete BFS until all nodes have been visited
            } else {
                exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
                aggregatorFunction = (s, t, w) -> .0;
            }

            validateStartNode(configuration.startNode(), graph);
            configuration.targetNodes().stream().forEach(neoId -> validateEndNode(neoId, graph));

            var mappedStartNodeId = graph.toMappedNodeId(configuration.startNode());

            return isBfs
                ? Traverse.bfs(graph, mappedStartNodeId, exitFunction, aggregatorFunction)
                : Traverse.dfs(graph, mappedStartNodeId, exitFunction, aggregatorFunction);
        };
    }

    private Stream<WalkResult> stream(Object graphNameOrConfig, Map<String, Object> configuration) {
        ComputationResult<Traverse, Traverse, TraverseConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        if (computationResult.graph().isEmpty()) {
            return Stream.empty();
        }

        Traverse traverse = computationResult.algorithm();
        long[] nodes = traverse.resultNodes();
        return Stream.of(new WalkResult(nodes, WalkPath.toPath(transaction, nodes)));
    }
}
