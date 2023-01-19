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

import java.util.List;

public class TestAdjacencyCursor implements AdjacencyCursor {

    private final List<Long> targetIds;
    private int cursor;

    TestAdjacencyCursor(List<Long> targetIds) {
        this.targetIds = targetIds;
    }

    @Override
    public void init(long index, int degree) {
        this.cursor = targetIds.indexOf(index);
    }

    @Override
    public int size() {
        return this.targetIds.size();
    }

    @Override
    public boolean hasNextVLong() {
        return cursor < targetIds.size() - 1;
    }

    @Override
    public long nextVLong() {
        var value = targetIds.get(cursor);
        cursor++;
        return value;
    }

    @Override
    public long peekVLong() {
        return targetIds.get(cursor);
    }

    @Override
    public int remaining() {
        return targetIds.size() - cursor - 1;
    }

    @Override
    public long skipUntil(long nodeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long advance(long nodeId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long advanceBy(int n) {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
        throw new UnsupportedOperationException();
    }
}
