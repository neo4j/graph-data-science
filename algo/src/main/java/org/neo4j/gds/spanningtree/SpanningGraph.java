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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphAdapter;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;

import java.util.Arrays;

public class SpanningGraph extends GraphAdapter {

    private final SpanningTree spanningTree;

    public SpanningGraph(Graph graph, SpanningTree spanningTree) {
        super(graph);
        this.spanningTree = spanningTree;
    }

    @Override
    public int degree(long nodeId) {
        if (spanningTree.parent.get(nodeId) == -1) {
            return Math.toIntExact(Arrays.stream(spanningTree.parent.toArray()).filter(i -> i == -1).count());
        } else {
            return 1;
        }
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        forEachRelationship(
            nodeId,
            0.0,
            (sourceNodeId, targetNodeId, property) -> consumer.accept(sourceNodeId, targetNodeId)
        );
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        long parent = spanningTree.parent.get(nodeId);
        if (parent != -1) {
            consumer.accept(parent, nodeId, spanningTree.costToParent(nodeId));
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return spanningTree.parent.get(sourceNodeId) != -1 || spanningTree.parent.get(targetNodeId) != -1;
    }

    @Override
    public Graph concurrentCopy() {
        return this;
    }
}
