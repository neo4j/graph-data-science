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
package org.neo4j.gds.core.compression.mixed;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.ImmutableMemoryInfo;
import org.neo4j.gds.core.compression.packed.AdjacencyPacking;

public class MixedAdjacencyList implements AdjacencyList {

    private final AdjacencyList packedAdjacencyList;

    private final AdjacencyList vlongAdjacencyList;

    MixedAdjacencyList(AdjacencyList packedAdjacencyList, AdjacencyList vlongAdjacencyList) {
        this.packedAdjacencyList = packedAdjacencyList;
        this.vlongAdjacencyList = vlongAdjacencyList;
    }

    @Override
    public int degree(long node) {
        return this.vlongAdjacencyList.degree(node);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        return vlongAdjacencyList.adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return vlongAdjacencyList.rawAdjacencyCursor();
    }

    @Override
    public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        // TODO share this constant on the compression side
        if (degree(node) > AdjacencyPacking.BLOCK_SIZE * 8) {
            this.packedAdjacencyList.adjacencyCursor(node, fallbackValue);
        }
        return vlongAdjacencyList.adjacencyCursor(reuse, node, fallbackValue);
    }

    @Override
    public MemoryInfo memoryInfo() {
        // TODO
        return MemoryInfo.EMPTY;
    }
}

