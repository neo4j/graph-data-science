/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.linkprediction;

import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;
import java.util.Set;

public class LinkPredictionFunc extends BaseProc {

    @UserFunction("gds.alpha.linkprediction.adamicAdar")
    @Description("Given two nodes, calculate Adamic Adar similarity")
    public double adamicAdarSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://en.wikipedia.org/wiki/Adamic/Adar_index

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder().findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / Math.log(degree(nb, relationshipType, direction))).sum();
    }

    @UserFunction("gds.alpha.linkprediction.resourceAllocation")
    @Description("Given two nodes, calculate Resource Allocation similarity")
    public double resourceAllocationSimilarity(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        // https://arxiv.org/pdf/0901.0553.pdf

        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder().findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.stream().mapToDouble(nb -> 1.0 / degree(nb, relationshipType, direction)).sum();
    }

    @UserFunction("gds.alpha.linkprediction.commonNeighbors")
    @Description("Given two nodes, returns the number of common neighbors")
    public double commonNeighbors(@Name("node1") Node node1, @Name("node2") Node node2,
                                               @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        Set<Node> neighbors = new NeighborsFinder().findCommonNeighbors(node1, node2, relationshipType, direction);
        return neighbors.size();
    }

    @UserFunction("gds.alpha.linkprediction.preferentialAttachment")
    @Description("Given two nodes, calculate Preferential Attachment")
    public double preferentialAttachment(@Name("node1") Node node1, @Name("node2") Node node2,
                                       @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        if (node1 == null || node2 == null) {
            throw new RuntimeException("Nodes must not be null");
        }

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        return degree(node1, relationshipType, direction) * degree(node2, relationshipType, direction);
    }

    @UserFunction("gds.alpha.linkprediction.totalNeighbors")
    @Description("Given two nodes, calculate Total Neighbors")
    public double totalNeighbors(@Name("node1") Node node1, @Name("node2") Node node2,
                                         @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        RelationshipType relationshipType = configuration.getRelationship();
        Direction direction = configuration.getDirection(Direction.BOTH);

        NeighborsFinder neighborsFinder = new NeighborsFinder();
        return neighborsFinder.findNeighbors(node1, node2, relationshipType, direction).size();
    }

    @UserFunction("gds.alpha.linkprediction.sameCommunity")
    @Description("Given two nodes, indicates if they have the same community")
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
