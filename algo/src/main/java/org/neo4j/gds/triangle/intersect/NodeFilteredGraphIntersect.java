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
package org.neo4j.gds.triangle.intersect;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IntersectionConsumer;
import org.neo4j.gds.api.RelationshipIntersect;
import org.neo4j.gds.core.huge.NodeFilteredGraph;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.gds.api.RelationshipIterator}s.
 */

public final class NodeFilteredGraphIntersect implements RelationshipIntersect {

    private final NodeFilteredGraph filteredGraph;
    private final RelationshipIntersect wrappedRelationshipIntersect;

    private NodeFilteredGraphIntersect(NodeFilteredGraph filteredGraph, RelationshipIntersect wrappedRelationshipIntersect) {
        this.filteredGraph = filteredGraph;
        this.wrappedRelationshipIntersect = wrappedRelationshipIntersect;
    }

    @Override
    public void intersectAll(long nodeIdA, IntersectionConsumer consumer) {
        wrappedRelationshipIntersect.intersectAll(filteredGraph.toRootNodeId(nodeIdA), (a, b, c) -> {
            if (filteredGraph.containsRootNodeId(a) && filteredGraph.containsRootNodeId(b) && filteredGraph.containsRootNodeId(c)) {
                consumer.accept(
                    filteredGraph.toFilteredNodeId(a),
                    filteredGraph.toFilteredNodeId(b),
                    filteredGraph.toFilteredNodeId(c)
                );
            }
        });
    }

    @ServiceProvider
    public static final class NodeFilteredGraphIntersectFactory implements RelationshipIntersectFactory {

        @Override
        public boolean canLoad(Graph graph) {
            return graph instanceof NodeFilteredGraph;
        }

        @Override
        public RelationshipIntersect load(Graph graph, RelationshipIntersectConfig config) {
            assert graph instanceof NodeFilteredGraph;
            var nodeFilteredGraph = (NodeFilteredGraph) graph;
            var innerGraph = nodeFilteredGraph.graph();

            var relationshipIntersect = RelationshipIntersectFactoryLocator
                .lookup(innerGraph)
                .orElseThrow(() -> new IllegalArgumentException("No intersect factory found for graph type " + innerGraph.getClass()))
                .load(innerGraph, config);

            return new NodeFilteredGraphIntersect(nodeFilteredGraph, relationshipIntersect);
        }
    }
}
