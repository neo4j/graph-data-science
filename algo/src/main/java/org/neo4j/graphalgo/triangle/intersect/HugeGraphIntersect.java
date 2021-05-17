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
package org.neo4j.graphalgo.triangle.intersect;

import org.jetbrains.annotations.Nullable;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.huge.HugeGraph;

public final class HugeGraphIntersect extends GraphIntersect<AdjacencyCursor> {

    private final AdjacencyList adjacencyList;

    private HugeGraphIntersect(AdjacencyList adjacency, long maxDegree) {
        super(maxDegree);
        this.adjacencyList = adjacency;
    }

    @Override
    protected int degree(long node) {
        return adjacencyList.degree(node);
    }

    @Override
    protected AdjacencyCursor checkCursorInstance(AdjacencyCursor cursor) {
        return cursor;
    }

    @Override
    protected AdjacencyCursor cursorForNode(@Nullable AdjacencyCursor reuse, long node, int degree) {
        return adjacencyList.adjacencyCursor(reuse, node);
    }

    @ServiceProvider
    public static final class HugeGraphIntersectFactory implements RelationshipIntersectFactory {

        @Override
        public boolean canLoad(Graph graph) {
            return graph instanceof HugeGraph;
        }

        @Override
        public RelationshipIntersect load(Graph graph, RelationshipIntersectConfig config) {
            assert graph instanceof HugeGraph;
            var hugeGraph = (HugeGraph) graph;
            var topology = hugeGraph.relationshipTopology().adjacencyList();
            return new HugeGraphIntersect(topology, config.maxDegree());
        }
    }
}
