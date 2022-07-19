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
package org.neo4j.gds.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Builds a grid of nodes
 *
 * A -- B -- C -- D -- E -- F ..
 * |    |    |    |    |    |
 * G -- H -- I -- J -- K -- L ..
 * |    |    |    |    |    |
 * ..   ..   ..   ..   ..   ..
 */
public class GridBuilder extends GraphBuilder<GridBuilder> {

    GridBuilder(Transaction tx, Label label, RelationshipType relationship, Random random) {
        super(tx, label, relationship, random);
    }

    public GridBuilder createGrid(int width, int height) {
        List<Node> temp = null;
        for (int i = 0; i < height; i++) {
            List<Node> line = createLine(width);
            if (null != temp) {
                for (int j = 0; j < width; j++) {
                    if (randomDouble() < 1.0) {
                        createRelationship(temp.get(j), line.get(j));
                    }
                }
            }
            temp = line;
        }
        return this;
    }

    private List<Node> createLine(int length) {
        List<Node> nodes = new ArrayList<>();
        Node temp = createNode();
        for (int i = 1; i < length; i++) {
            Node node = createNode();
            nodes.add(temp);
            createRelationship(temp, node);
            temp = node;
        }
        nodes.add(temp);
        return nodes;
    }

    @Override
    protected GridBuilder me() {
        return this;
    }
}
