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
package org.neo4j.graphalgo.impl.spanningTrees;

import org.neo4j.graphalgo.api.FilterGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;

public class SpanningGraph extends FilterGraph {

    private final SpanningTree spanningTree;

    public SpanningGraph(Graph graph, SpanningTree spanningTree) {
        super(graph);
        this.spanningTree = spanningTree;
    }

    @Override
    public int degree(long nodeId, Direction direction) {
        if (spanningTree.parent[Math.toIntExact(nodeId)] == -1) {
            return Math.toIntExact(Arrays.stream(spanningTree.parent).filter(i -> i == -1).count());
        } else {
            return 1;
        }
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        forEachRelationship(
            nodeId,
            direction,
            0.0,
            (sourceNodeId, targetNodeId, property) -> consumer.accept(sourceNodeId, targetNodeId)
        );
    }

    @Override
    public void forEachRelationship(
        long nodeId,
        Direction direction,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {
        int parent = spanningTree.parent[Math.toIntExact(nodeId)];
        if (parent != -1) {
            consumer.accept(parent, nodeId, fallbackValue);
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        int source = Math.toIntExact(sourceNodeId);
        int target = Math.toIntExact(targetNodeId);
        return spanningTree.parent[source] != -1 || spanningTree.parent[target] != -1;
    }
}
