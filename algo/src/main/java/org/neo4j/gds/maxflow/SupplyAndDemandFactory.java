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
package org.neo4j.gds.maxflow;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;

import java.util.Arrays;

public final class SupplyAndDemandFactory {

    private SupplyAndDemandFactory() {}

    public static Pair<NodeWithValue[], NodeWithValue[]> create(
        Graph graph,
        InputNodes sourceNodes,
        InputNodes targetNodes
    ) {
        var supply = createSupply(sourceNodes, graph);
        var demand = createDemand(targetNodes, graph, supply);
        return Pair.of(supply, demand);
    }

    private static NodeWithValue[] createSupply(InputNodes sourceNodes, Graph graph) {
        return
            switch (sourceNodes) {
                case ListInputNodes list -> list.inputNodes().stream().map(sourceNode -> new NodeWithValue(
                    graph.toMappedNodeId(sourceNode), graph.streamRelationships(
                    graph.toMappedNodeId(sourceNode),
                    0D
                ).map(RelationshipCursor::property).reduce(0D, Double::sum)
                )).toArray(NodeWithValue[]::new);
                case MapInputNodes map -> map.map().entrySet().stream().map(entry -> new NodeWithValue(
                    graph.toMappedNodeId(entry.getKey()),
                    entry.getValue()
                )).toArray(NodeWithValue[]::new);
                default -> throw new IllegalStateException("Unexpected value: " + sourceNodes);
            };
    }

    private static NodeWithValue[] createDemand(InputNodes targetNodes, Graph graph, NodeWithValue[] supply) {
        return
            switch (targetNodes) {
                case ListInputNodes list -> {
                    var totalOutgoing = Arrays.stream(supply).map(nodeWithValue -> nodeWithValue.value()).reduce(0D, Double::sum);
                    yield list.inputNodes().stream().map(sourceNode -> new NodeWithValue(graph.toMappedNodeId(sourceNode), totalOutgoing)).toArray(NodeWithValue[]::new);
                }
                case MapInputNodes map -> map.map().entrySet().stream().map(entry -> new NodeWithValue(
                    graph.toMappedNodeId(entry.getKey()),
                    entry.getValue()
                )).toArray(NodeWithValue[]::new);
                default -> throw new IllegalStateException("Unexpected value: " + targetNodes);
            };
    }
}
