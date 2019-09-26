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
package org.neo4j.graphalgo.graphbuilder;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Random;

/**
 * Builds a complete graph where all nodes are interconnected
 *
 * @author mknblch
 */
public class CompleteGraphBuilder extends GraphBuilder<CompleteGraphBuilder> {

    CompleteGraphBuilder(GraphDatabaseAPI api, Label label, RelationshipType relationship, Random random) {
        super(api, label, relationship, random);
    }

    public CompleteGraphBuilder createCompleteGraph(int nodeCount) {
        ArrayList<Node> nodes = new ArrayList<>();
        withinTransaction(() -> {

            for (int i = 0; i < nodeCount; i++) {
                nodes.add(createNode());
            }
            for (int i = 0; i < nodeCount; i++) {
                for (int j = 0; j < nodeCount; j++) {
                    if (i == j) {
                        continue;
                    }
                    createRelationship(nodes.get(i), nodes.get(j));
                }
            }
        });
        return this;
    }


    public CompleteGraphBuilder createCompleteGraph(int nodeCount, double connectedness) {
        ArrayList<Node> nodes = new ArrayList<>();
        withinTransaction(() -> {

            for (int i = 0; i < nodeCount; i++) {
                nodes.add(createNode());
            }
            for (int i = 0; i < nodeCount; i++) {
                for (int j = 0; j < nodeCount; j++) {
                    if (i == j) {
                        continue;
                    }
                    if (randomDouble() < connectedness) {
                        createRelationship(nodes.get(i), nodes.get(j));
                    }
                }
            }
        });
        return this;
    }

    @Override
    protected CompleteGraphBuilder me() {
        return this;
    }
}
