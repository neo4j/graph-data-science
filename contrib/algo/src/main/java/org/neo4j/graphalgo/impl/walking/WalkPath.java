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
package org.neo4j.graphalgo.impl.walking;


import org.neo4j.graphalgo.results.VirtualRelationship;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class WalkPath implements Path {
    public static final Path EMPTY = new WalkPath(0);
    private static final RelationshipType NEXT = RelationshipType.withName("NEXT");

    private List<Node> nodes;
    private List<Relationship> relationships;
    private final int size;

    public WalkPath(int size) {
        nodes = new ArrayList<>(size);
        relationships = new ArrayList<>(Math.max(0, size - 1)); // for empty paths
        this.size = size;
    }

    public static Path toPath(GraphDatabaseAPI api, long[] nodes) {
        if (nodes.length == 0) return EMPTY;
        WalkPath result = new WalkPath(nodes.length);
        Node node = api.getNodeById(nodes[0]);
        result.addNode(node);
        for (int i = 1; i < nodes.length; i++) {
            Node nextNode = api.getNodeById(nodes[i]);
            result.addRelationship(new VirtualRelationship(node, nextNode, NEXT));
            result.addNode(nextNode);
            node = nextNode;
        }
        return result;
    }

    public static Path toPath(GraphDatabaseAPI api, long[] nodes, double[] costs) {
        if (nodes.length == 0) return EMPTY;
        WalkPath result = new WalkPath(nodes.length);
        Node node = api.getNodeById(nodes[0]);
        result.addNode(node);
        for (int i = 1; i < nodes.length; i++) {
            Node nextNode = api.getNodeById(nodes[i]);
            VirtualRelationship relationship = new VirtualRelationship(node, nextNode, NEXT);
            relationship.setProperty("cost", costs[i-1]);
            result.addRelationship(relationship);
            result.addNode(nextNode);
            node = nextNode;
        }
        return result;
    }

    public void addNode(Node node) {
        nodes.add(node);
    }

    public void addRelationship(Relationship relationship) {
        relationships.add(relationship);
    }

    @Override
    public Node startNode() {
        return size==0 ? null : nodes.get(0);
    }

    @Override
    public Node endNode() {
        return size==0 ? null : nodes.get(nodes.size() - 1);
    }

    @Override
    public Relationship lastRelationship() {
        return size==0 ? null : relationships.get(relationships.size() - 1);
    }

    @Override
    public Iterable<Relationship> relationships() {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships() {
        ArrayList<Relationship> reverse = new ArrayList<>(relationships);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public Iterable<Node> nodes() {
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes() {
        ArrayList<Node> reverse = new ArrayList<>(nodes);
        Collections.reverse(reverse);
        return reverse;
    }

    @Override
    public int length() {
        return size-1;
    }

    @Override
    public String toString() {
        return nodes.toString();
    }

    @Override
    public Iterator<PropertyContainer> iterator() {
        return new Iterator<PropertyContainer>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i < 2 * size;
            }

            @Override
            public PropertyContainer next() {
                PropertyContainer pc = i % 2 == 0 ? nodes.get(i / 2) : relationships.get(i / 2);
                i++;
                return pc;
            }
        };
    }
}
