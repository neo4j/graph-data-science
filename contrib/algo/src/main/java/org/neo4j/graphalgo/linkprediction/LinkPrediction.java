/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.linkprediction;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;
import java.util.Set;

public class LinkPrediction {
    @Context
    public GraphDatabaseAPI api;

    @UserFunction("algo.linkprediction.adamicAdar")
    @Description("algo.linkprediction.adamicAdar(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Adamic Adar similarity")
    public double adamicAdarSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://en.wikipedia.org/wiki/Adamic/Adar_index

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / Math.log(degree(nb, relationshipType, direction))).sum();
    }

    @UserFunction("algo.linkprediction.resourceAllocation")
    @Description("algo.linkprediction.resourceAllocation(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Resource Allocation similarity")
    public double resourceAllocationSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://arxiv.org/pdf/0901.0553.pdf

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / degree(nb, relationshipType, direction)).sum();
    }

    @UserFunction("algo.linkprediction.commonNeighbors")
    @Description("algo.linkprediction.commonNeighbors(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, returns the number of common neighbors")
    public double commonNeighbors(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder(api).findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.size();
    }

    @UserFunction("algo.linkprediction.preferentialAttachment")
    @Description("algo.linkprediction.preferentialAttachment(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Preferential Attachment")
    public double preferentialAttachment(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        return degree(node1, relationshipType, direction) * degree(node2, relationshipType, direction);
    }

    @UserFunction("algo.linkprediction.totalNeighbors")
    @Description("algo.linkprediction.totalNeighbors(node1:Node, node2:Node, {relationshipQuery:'relationshipName', direction:'BOTH'}) " +
            "given two nodes, calculate Total Neighbors")
    public double totalNeighbors(@Name("node1") Node node1, @Name("node2") Node node2,
                                         @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        NeighborsFinder neighborsFinder = new NeighborsFinder(api);
        return neighborsFinder.findNeighbors(node1, node2, relationshipType, direction).size();
    }

    @UserFunction("algo.linkprediction.sameCommunity")
    @Description("algo.linkprediction.sameCommunity(node1:Node, node2:Node, communityProperty: String) " +
            "given two nodes, indicates if they have the same community")
    public double sameCommunity(@Name("node1") Node node1, @Name("node2") Node node2,
                                 @Name(value = "communityProperty", defaultValue = "community") String communityProperty) {
        if(!node1.hasProperty(communityProperty) || !node2.hasProperty(communityProperty)) {
            return 0.0;        }

        return node1.getProperty(communityProperty).equals(node2.getProperty(communityProperty)) ? 1.0 : 0.0;
    }

    private int degree(Node node, RelationshipType relationshipType, Direction direction) {
        return relationshipType == null ? node.getDegree(direction) : node.getDegree(relationshipType, direction);
    }

}
