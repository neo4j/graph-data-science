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

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.FilteredIdMap;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodeFilteredAdjacencyCursor implements AdjacencyCursor {

    private final AdjacencyCursor innerCursor;
    private final FilteredIdMap idMap;

    private long currentLongValue;

    public NodeFilteredAdjacencyCursor(AdjacencyCursor innerCursor, FilteredIdMap idMap) {
        this.innerCursor = innerCursor;
        this.idMap = idMap;
        hasNextVLong();
    }

    @Override
    public void init(long index, int degree) {
        innerCursor.init(index, degree);
        hasNextVLong();
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
        this.currentLongValue = AdjacencyCursor.NOT_FOUND;
        if (innerCursor.hasNextVLong()) {
            var innerNextLong = innerCursor.peekVLong();
            if (!idMap.containsRootNodeId(innerNextLong)) {
                innerCursor.nextVLong();
                return hasNextVLong();
            }
            this.currentLongValue = idMap.toFilteredNodeId(innerNextLong);
            return true;
        }
        return false;
    }

    @Override
    public long nextVLong() {
        var valueAtReturn = this.currentLongValue;
        if (innerCursor.hasNextVLong()) {
            innerCursor.nextVLong();
        }
        hasNextVLong();
        return valueAtReturn;
    }

    @Override
    public long peekVLong() {
        return this.currentLongValue;
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
        while (currentLongValue <= nodeId) {
            if (hasNextVLong()) {
                nextVLong();
            } else {
                return AdjacencyCursor.NOT_FOUND;
            }
        }
        return currentLongValue;
    }

    @Override
    public long advance(long nodeId) {
        while (currentLongValue < nodeId) {
            if (hasNextVLong()) {
                nextVLong();
            } else {
                return AdjacencyCursor.NOT_FOUND;
            }
        }
        return currentLongValue;
    }

    @Override
    public long advanceBy(int n) {
        assert n >= 0;

        while (hasNextVLong()) {
            nextVLong();
            if (n-- == 0) {
                return currentLongValue;
            }
        }
        return NOT_FOUND;
    }

}
