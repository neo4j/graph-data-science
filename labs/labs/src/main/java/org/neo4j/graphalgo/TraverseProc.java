/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.Traverse;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class TraverseProc extends LabsProc {

    @Procedure(name = "algo.bfs.stream", mode = READ)
    @Description("CALL algo.bfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<WalkResult> bfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") String direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setDirection(direction)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withRelationshipProperties(PropertyMapping.of(configuration.getWeightProperty(), 1.))
                .withDirection(configuration.getDirection(Direction.OUTGOING))
                .withLog(log)
                .load(configuration.getGraphImpl());

        final Traverse traverse = new Traverse(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        final List<Long> targetNodes = configuration.get("targetNodes", Collections.emptyList());
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final Traverse.ExitPredicate exitFunction;
        final Traverse.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (!targetNodes.isEmpty()) {
            final List<Long> mappedTargets = targetNodes.stream()
                    .map(graph::toMappedNodeId)
                    .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w >= maxDepth ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKey("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.relationshipProperty(s, t, 0.0D);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traverse.computeBfs(
                startNode,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(new WalkResult(nodes, WalkPath.toPath(api, nodes)));
    }

    @Procedure(name = "algo.dfs.stream", mode = READ)
    @Description("CALL algo.dfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<WalkResult> dfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") String direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername())
                .setDirection(direction)
                .setNodeLabelOrQuery(label)
                .setRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withRelationshipProperties(PropertyMapping.of(configuration.getWeightProperty(), 1.0))
                .withDirection(configuration.getDirection(Direction.OUTGOING))
                .withLog(log)
                .load(configuration.getGraphImpl());

        final Traverse traverse = new Traverse(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "DFS"))
                .withTerminationFlag(TerminationFlag.wrap(transaction));

        final List<Long> targetNodes = configuration.get("targetNodes", Collections.emptyList());
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final Traverse.ExitPredicate exitFunction;
        final Traverse.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (!targetNodes.isEmpty()) {
            final List<Long> mappedTargets = targetNodes.stream()
                    .map(graph::toMappedNodeId)
                    .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;

        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w > maxDepth ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKey("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.relationshipProperty(s, t, 0.0D);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traverse.computeDfs(
                startNode,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(new WalkResult(nodes, WalkPath.toPath(api, nodes)));
    }

}
