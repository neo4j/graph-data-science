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
package org.neo4j.gds.core.huge;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.IdMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeFilteredAdjacencyCursor implements AdjacencyCursor {

    private final AdjacencyCursor innerCursor;
    private final IdMap idMap;

    private long nextLongValue;

    NodeFilteredAdjacencyCursor(AdjacencyCursor innerCursor, IdMap idMap) {
        this.innerCursor = innerCursor;
        this.idMap = idMap;

        this.nextLongValue = -1L;
    }

    @Override
    public void init(long index, int degree) {
        innerCursor.init(index, degree);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException(formatWithLocale(
            "AdjacencyCursor#size is not implemented for %s",
            getClass().getSimpleName()
        ));
    }

    @Override
    public boolean hasNextVLong() {
        if (innerCursor.hasNextVLong()) {
            var innerNextLong = innerCursor.peekVLong();
            if (!idMap.contains(innerNextLong)) {
                innerCursor.nextVLong();
                return hasNextVLong();
            }
            this.nextLongValue = idMap.toMappedNodeId(innerNextLong);
            return true;
        }
        return false;
    }

    @Override
    public long nextVLong() {
        if (innerCursor.hasNextVLong()) {
            innerCursor.nextVLong();
        }
        return this.nextLongValue;
    }

    @Override
    public long peekVLong() {
        return this.nextLongValue;
    }

    @Override
    public int remaining() {
        throw new UnsupportedOperationException(formatWithLocale(
            "AdjacencyCursor#remaining is not implemented for %s",
            getClass().getSimpleName()
        ));
    }

    @Override
    public long skipUntil(long nodeId) {
        while (nextLongValue <= nodeId) {
            if (hasNextVLong()) {
                nextVLong();
            } else {
                return AdjacencyCursor.NOT_FOUND;
            }
        }
        return nextLongValue;
    }

    @Override
    public long advance(long nodeId) {
        while (nextLongValue < nodeId) {
            if (hasNextVLong()) {
                nextVLong();
            } else {
                return AdjacencyCursor.NOT_FOUND;
            }
        }
        return nextLongValue;
    }

    @Override
    public long advanceBy(int n) {
        assert n >= 0;

        while (hasNextVLong()) {
            nextVLong();
            if (n-- == 0) {
                return nextLongValue;
            }
        }
        return NOT_FOUND;
    }

    @Override
    public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
        return new NodeFilteredAdjacencyCursor(innerCursor.shallowCopy(destination), idMap);
    }

    @Override
    public void close() {
        innerCursor.close();
    }
}
