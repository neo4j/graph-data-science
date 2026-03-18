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
package org.neo4j.gds.triangle;

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.NodeFilteredAdjacencyCursor;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.huge.UnionGraph;

/** Selects sorted adjacency cursor access for each graph representation. */
@FunctionalInterface
interface TriangleCentralityAdjacencyCursorFactory {

    AdjacencyCursor cursorForNode(AdjacencyCursor reuse, long node);

    static TriangleCentralityAdjacencyCursorFactory forGraph(Graph graph) {
        return switch (graph) {
            case HugeGraph hugeGraph -> forHugeGraph(hugeGraph);
            case UnionGraph unionGraph -> unionGraph.isNodeFilteredGraph()
                ? forNodeFilteredUnionGraph(unionGraph)
                : forUnionGraph(unionGraph);
            case NodeFilteredGraph nodeFilteredGraph -> forNodeFilteredGraph(nodeFilteredGraph);
            default -> throw new IllegalArgumentException(
                "Triangle Centrality does not support graph type: " + graph.getClass()
            );
        };
    }

    private static TriangleCentralityAdjacencyCursorFactory forHugeGraph(HugeGraph graph) {
        return graph.relationshipTopology().adjacencyList()::adjacencyCursor;
    }

    private static TriangleCentralityAdjacencyCursorFactory forUnionGraph(UnionGraph graph) {
        return graph.relationshipTopology()::adjacencyCursor;
    }

    private static TriangleCentralityAdjacencyCursorFactory forNodeFilteredUnionGraph(UnionGraph graph) {
        var adjacencyList = graph.relationshipTopology();
        // The composite cursor maps targets to filtered IDs, but opens lists by root ID.
        return (reuse, node) -> adjacencyList.adjacencyCursor(reuse, graph.toRootNodeId(node));
    }

    private static TriangleCentralityAdjacencyCursorFactory forNodeFilteredGraph(NodeFilteredGraph graph) {
        var rootAdjacency = forGraph(graph.graph());
        return (reuse, node) -> {
            var innerReuse = reuse instanceof NodeFilteredAdjacencyCursor filteredCursor
                ? filteredCursor.innerCursor()
                : null;
            var innerCursor = rootAdjacency.cursorForNode(innerReuse, graph.toRootNodeId(node));
            if (innerCursor == innerReuse) {
                // Refresh the filtered lookahead after the underlying cursor has been reset.
                reuse.hasNextVLong();
                return reuse;
            }
            return new NodeFilteredAdjacencyCursor(innerCursor, graph);
        };
    }
}
