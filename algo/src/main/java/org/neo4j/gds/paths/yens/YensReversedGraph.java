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
package org.neo4j.gds.paths.yens;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphAdapter;
import org.neo4j.gds.api.properties.relationships.RelationshipConsumer;
import org.neo4j.gds.api.properties.relationships.RelationshipWithPropertyConsumer;

public class YensReversedGraph extends GraphAdapter {
    public YensReversedGraph(Graph graph) {
        super(graph);
    }

    @Override
    public Graph concurrentCopy() {
        return new YensReversedGraph(graph.concurrentCopy());
    }

    @Override
    public int degree(long nodeId) {
        return graph.degreeInverse(nodeId);
    }

    @Override
    public int degreeInverse(long nodeId) {
        return graph.degree(nodeId);
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        graph.forEachInverseRelationship(nodeId, consumer);
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        graph.forEachInverseRelationship(nodeId, fallbackValue, consumer);
    }

    @Override
    public void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer) {
        graph.forEachRelationship(nodeId, consumer);
    }

    @Override
    public void forEachInverseRelationship(
        long nodeId,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {
        graph.forEachRelationship(nodeId, fallbackValue, consumer);
    }
}
