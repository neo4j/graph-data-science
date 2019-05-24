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
package org.neo4j.graphalgo.helper.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Random;

/**
 * Create a line of nodes where each node is only connected to its successor
 *
 * @author mknblch
 */
public class LineBuilder extends GraphBuilder<LineBuilder> {

    private Node head, tail;

    LineBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship, Random random) {
        super(api, label, relationship, random);
    }

    public LineBuilder withHead(Node head) {
        this.head = head;
        nodes.add(head);
        return this;
    }

    public LineBuilder withTail(Node tail) {
        this.tail = tail;
        nodes.add(tail);
        return this;
    }

    /**
     * create a ring
     *
     * @param size number of elements (&gt;= 2)
     * @return itself for method chaining
     */
    public LineBuilder createLine(int size) {
        if (size < 2) {
            throw new IllegalArgumentException("size must be >= 2");
        }
        withinTransaction(() -> {
            head = head == null ? createNode() : head;
            tail = tail == null ? createNode() : tail;
            Node temp = head;
            for (int i = 2; i < size; i++) {
                Node node = createNode();
                createRelationship(temp, node);
                temp = node;
            }
            createRelationship(temp, tail);
        });
        return this;
    }

    @Override
    protected LineBuilder me() {
        return this;
    }
}
