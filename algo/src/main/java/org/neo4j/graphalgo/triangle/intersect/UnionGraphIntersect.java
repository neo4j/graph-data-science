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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.huge.CompositeAdjacencyCursor;
import org.neo4j.graphalgo.core.huge.CompositeAdjacencyDegrees;
import org.neo4j.graphalgo.core.huge.CompositeAdjacencyList;
import org.neo4j.graphalgo.core.huge.UnionGraph;

import java.util.ArrayList;

public final class UnionGraphIntersect extends GraphIntersect<CompositeAdjacencyCursor> {

    private final CompositeAdjacencyDegrees compositeAdjacencyDegrees;
    private final CompositeAdjacencyList compositeAdjacencyList;

    private UnionGraphIntersect(
        CompositeAdjacencyDegrees compositeAdjacencyDegrees,
        CompositeAdjacencyList compositeAdjacencyList,
        long maxDegree
    ) {
        super(
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            maxDegree
        );
        this.compositeAdjacencyDegrees = compositeAdjacencyDegrees;
        this.compositeAdjacencyList = compositeAdjacencyList;
    }

    @Override
    protected CompositeAdjacencyCursor cursor(long nodeId, int unusedDegree, CompositeAdjacencyCursor reuse) {
        var adjacencyCursors = new ArrayList<AdjacencyCursor>(compositeAdjacencyList.adjacencyLists().size());
        var cursors = reuse.cursors();
        var emptyCursors = empty.cursors();

        compositeAdjacencyList.forEachOffset(
            nodeId,
            (list, index, offset, degree, hasAdjacency) -> adjacencyCursors.add(
                index,
                hasAdjacency
                    ? cursors.get(index).initializedTo(offset, degree)
                    : emptyCursors.get(index)
            )
        );
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    protected int degree(long nodeId) {
        return compositeAdjacencyDegrees.degree(nodeId);
    }

    @ServiceProvider
    public static final class UnionGraphIntersectFactory implements RelationshipIntersectFactory {

        @Override
        public boolean canLoad(Graph graph) {
            return graph instanceof UnionGraph;
        }

        @Override
        public RelationshipIntersect load(Graph graph, RelationshipIntersectConfig config) {
            assert graph instanceof UnionGraph;
            var topology = ((UnionGraph) graph).relationshipTopology();
            return new UnionGraphIntersect(
                (CompositeAdjacencyDegrees) topology.degrees(),
                (CompositeAdjacencyList) topology.list(),
                config.maxDegree()
            );
        }
    }
}
