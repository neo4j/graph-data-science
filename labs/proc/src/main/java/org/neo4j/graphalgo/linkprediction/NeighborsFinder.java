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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.neo4j.graphdb.Direction.*;

public class NeighborsFinder {

    private GraphDatabaseAPI api;

    public NeighborsFinder(GraphDatabaseAPI api) {
        this.api = api;
    }

    public Set<Node> findCommonNeighbors(Node node1, Node node2, RelationshipType relationshipType, Direction direction) {
        if(node1.equals(node2)) {
            return Collections.emptySet();
        }

        Set<Node> neighbors = findNeighbors(node1, relationshipType, direction);
        neighbors.removeIf(node -> noCommonNeighbors(node, relationshipType, flipDirection(direction), node2));
        return neighbors;
    }

    public Set<Node> findNeighbors(Node node1, Node node2, RelationshipType relationshipType, Direction direction) {
        Set<Node> node1Neighbors = findNeighbors(node1, relationshipType, direction);
        Set<Node> node2Neighbors = findNeighbors(node2, relationshipType, direction);
        node1Neighbors.addAll(node2Neighbors);
        return node1Neighbors;
    }

    public Set<Node> findNeighbors(Node node, RelationshipType relationshipType, Direction direction) {
        Set<Node> neighbors = new HashSet<>();

        for (Relationship rel : loadRelationships(node, relationshipType, direction)) {
            Node endNode = rel.getOtherNode(node);

            if (!endNode.equals(node)) {
                neighbors.add(endNode);
            }
        }
        return neighbors;
    }

    private Direction flipDirection(Direction direction) {
        switch(direction) {
            case OUTGOING:
                return INCOMING;
            case INCOMING:
                return OUTGOING;
            default:
                return BOTH;
        }
    }

    private boolean noCommonNeighbors(Node node, RelationshipType relationshipType, Direction direction, Node node2) {
        for (Relationship rel : loadRelationships(node, relationshipType, direction)) {
            if (rel.getOtherNode(node).equals(node2)) {
                return false;
            }
        }
        return true;
    }

    private Iterable<Relationship> loadRelationships(Node node, RelationshipType relationshipType, Direction direction) {
        return relationshipType == null ? node.getRelationships(direction) : node.getRelationships(relationshipType, direction);
    }

}
