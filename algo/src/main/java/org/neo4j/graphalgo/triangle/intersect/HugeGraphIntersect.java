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

import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;

public class HugeGraphIntersect extends GraphIntersect<AdjacencyCursor> {

    private final AdjacencyList adjacency;
    private final AdjacencyOffsets offsets;

    public HugeGraphIntersect(final AdjacencyList adjacency, final AdjacencyOffsets offsets, long maxDegree) {
        super(
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            maxDegree
        );
        this.adjacency = adjacency;
        this.offsets = offsets;
    }

    @Override
    public AdjacencyCursor cursor(long node, AdjacencyCursor reuse) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return super.cursor(offset, reuse);
    }

    @Override
    protected int degree(long node) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return adjacency.degree(offset);
    }
}
