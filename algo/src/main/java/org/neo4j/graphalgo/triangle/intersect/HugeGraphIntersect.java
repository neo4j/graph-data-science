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
import org.neo4j.graphalgo.api.AdjacencyDegrees;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.huge.HugeGraph;

public final class HugeGraphIntersect extends GraphIntersect<AdjacencyCursor> {

    private final AdjacencyDegrees degrees;
    private final AdjacencyOffsets offsets;

    private HugeGraphIntersect(
        AdjacencyDegrees degrees,
        AdjacencyList adjacency,
        AdjacencyOffsets offsets,
        long maxDegree
    ) {
        super(adjacency::rawDecompressingCursor, maxDegree);
        this.degrees = degrees;
        this.offsets = offsets;
    }

    @Override
    public AdjacencyCursor cursor(long node, int degree, AdjacencyCursor reuse) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return super.cursor(offset, degree, reuse);
    }

    @Override
    protected int degree(long node) {
        return degrees.degree(node);
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
            var topology = hugeGraph.relationshipTopology();
            return new HugeGraphIntersect(topology.degrees(), topology.list(), topology.offsets(), config.maxDegree());
        }
    }
}
