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
package org.neo4j.gds.core.compression.packed;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public class PackedAdjacencyList implements AdjacencyList {

    private final HugeObjectArray<Compressed> adjacencies;

    PackedAdjacencyList(HugeObjectArray<Compressed> adjacencies) {
        this.adjacencies = adjacencies;
    }

    @Override
    public int degree(long node) {
        return adjacencies.get(node).length();
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        var degree = this.degree(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }

        var cursor = new DecompressingCursor(this.adjacencies, PackedCompressor.FLAGS);
        cursor.init(node);

        return cursor;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        var degree = this.degree(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }
        if (reuse instanceof DecompressingCursor) {
            ((DecompressingCursor) reuse).init(node);
            return reuse;
        }
        return adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return new DecompressingCursor(this.adjacencies, PackedCompressor.FLAGS);
    }
}
