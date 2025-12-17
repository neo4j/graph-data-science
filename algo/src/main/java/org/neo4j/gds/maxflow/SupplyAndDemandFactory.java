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
import org.neo4j.gds.InputNodes;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.relationships.RelationshipCursor;
import org.neo4j.gds.api.properties.relationships.RelationshipIterator;

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

    public static  Pair<NodeWithValue[], NodeWithValue[]> create(
        Graph graph,
        NodePropertyValues nodePropertyValues,
        InputNodes sourceNodes,
        InputNodes targetNodes
        ){

        var supply = createSupply(sourceNodes, graph,nodePropertyValues);
        var demand = createDemand(targetNodes, graph, supply,nodePropertyValues);
        return Pair.of(supply, demand);

    }

    private static double outSum(long nodeId, RelationshipIterator graph){
        return graph.streamRelationships(
            nodeId,
            0D
        ).map(RelationshipCursor::property).reduce(0D, Double::sum);
    }
    private static NodeWithValue[] createSupply(InputNodes sourceNodes, Graph graph) {
        return
            switch (sourceNodes) {
                case ListInputNodes list -> list.inputNodes().stream().map(sourceNode -> new NodeWithValue(
                    graph.toMappedNodeId(sourceNode), outSum(graph.toMappedNodeId(sourceNode),graph)
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
                    var totalOutgoing = Arrays.stream(supply).map(NodeWithValue::value).reduce(0D, Double::sum);
                    yield list.inputNodes().stream().map(sourceNode -> new NodeWithValue(graph.toMappedNodeId(sourceNode), totalOutgoing)).toArray(NodeWithValue[]::new);
                }
                case MapInputNodes map -> map.map().entrySet().stream().map(entry -> new NodeWithValue(
                    graph.toMappedNodeId(entry.getKey()),
                    entry.getValue()
                )).toArray(NodeWithValue[]::new);
                default -> throw new IllegalStateException("Unexpected value: " + targetNodes);
            };
    }

    private static NodeWithValue[] createSupply(InputNodes sourceNodes, Graph graph,NodePropertyValues nodePropertyValues) {
        return
            switch (sourceNodes) {
                case ListInputNodes list -> list.inputNodes().stream().map(sourceNode -> {
                    var  nodeId = graph.toMappedNodeId(sourceNode);
                    var value = nodePropertyValues.doubleValue(nodeId);
                    if (value < 0){
                        throw new IllegalArgumentException("Source node values must be positive, but found a negative value.");
                    }
                    if (Double.isNaN(value)) value =  outSum(nodeId, graph);
                   return new NodeWithValue(
                        graph.toMappedNodeId(sourceNode), value
                    );
                }).toArray(NodeWithValue[]::new);
                case MapInputNodes map -> map.map().entrySet().stream().map(entry ->
                {
                    var nodeId = graph.toMappedNodeId(entry.getKey());
                    var value = nodePropertyValues.doubleValue(nodeId);
                    if (!Double.isNaN(value)){
                        throw new IllegalArgumentException("Passing source node constraints via both `nodeCapacityProperty` and map is not accepted");
                    }
                    return new NodeWithValue(
                        graph.toMappedNodeId(nodeId), entry.getValue()
                    );
                }
                ).toArray(NodeWithValue[]::new);
                default -> throw new IllegalStateException("Unexpected value: " + sourceNodes);
            };
    }
    private static NodeWithValue[] createDemand(InputNodes targetNodes, Graph graph, NodeWithValue[] supply,NodePropertyValues nodePropertyValues) {
        return
            switch (targetNodes) {
                case ListInputNodes list -> {
                    var totalOutgoing = Arrays.stream(supply).map(NodeWithValue::value).reduce(0D, Double::sum);
                    yield list.inputNodes().stream().map(sourceNode -> {
                        var nodeId = graph.toMappedNodeId(sourceNode);
                        var value = nodePropertyValues.doubleValue(nodeId);
                        if (value < 0){
                            throw new IllegalArgumentException("Target node values must be positive, but found a negative value.");
                        }
                        if (Double.isNaN(value)){
                            value= totalOutgoing;
                        }
                        return new NodeWithValue(nodeId, value);

                    }).toArray(NodeWithValue[]::new);
                }
                case MapInputNodes map -> map.map().entrySet().stream().map(entry -> {
                        var nodeId = graph.toMappedNodeId(entry.getKey());
                        var value = nodePropertyValues.doubleValue(nodeId);
                        if (!Double.isNaN(value)){
                            throw new IllegalArgumentException("Passing target node constraints via both `nodeCapacityProperty` and map is not accepted");
                        }
                        return new NodeWithValue(
                            graph.toMappedNodeId(nodeId), value
                        );
                }
                ).toArray(NodeWithValue[]::new);
                default -> throw new IllegalStateException("Unexpected value: " + targetNodes);
            };
    }


}
