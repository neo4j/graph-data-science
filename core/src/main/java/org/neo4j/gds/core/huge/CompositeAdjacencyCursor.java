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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class CompositeAdjacencyCursor implements AdjacencyCursor {

    private final PriorityQueue<AdjacencyCursor> cursorQueue;
    private final List<AdjacencyCursor> cursors;

    CompositeAdjacencyCursor(List<AdjacencyCursor> cursors) {
        this.cursors = cursors;
        this.cursorQueue = new PriorityQueue<>(cursors.size(), Comparator.comparingLong(AdjacencyCursor::peekVLong));

        initializeQueue();
    }

    private void initializeQueue() {
        cursors.forEach(cursor -> {
            if (cursor != null && cursor.hasNextVLong()) {
                cursorQueue.add(cursor);
            }
        });
    }

    public List<AdjacencyCursor> cursors() {
        return cursors;
    }

    @Override
    public int size() {
        int sum = 0;
        for (var cursor : cursors) {
            int size = cursor.size();
            sum += size;
        }
        return sum;
    }

    @Override
    public boolean hasNextVLong() {
        return !cursorQueue.isEmpty();
    }

    @Override
    public long nextVLong() {
        AdjacencyCursor current = cursorQueue.poll();
        long targetNodeId = current.nextVLong();
        if (current.hasNextVLong()) {
            cursorQueue.add(current);
        }
        return targetNodeId;
    }

    @Override
    public long peekVLong() {
        return cursorQueue.peek().peekVLong();
    }

    @Override
    public int remaining() {
        int sum = 0;
        for (var cursor : cursors) {
            int remaining = cursor.remaining();
            sum += remaining;
        }
        return sum;
    }

    @Override
    public long skipUntil(long target) {
        for (var cursor : cursors) {
            cursorQueue.remove(cursor);
            // an implementation aware cursor would probably be much faster and could skip whole blocks
            // see AdjacencyDecompressingReader#skipUntil
            while (cursor.hasNextVLong() && cursor.peekVLong() <= target) {
                cursor.nextVLong();
            }
            if (cursor.hasNextVLong()) {
                cursorQueue.add(cursor);
            }
        }

        return cursorQueue.isEmpty() ? AdjacencyCursor.NOT_FOUND : nextVLong();
    }

    @Override
    public long advance(long target) {
        for (var cursor : cursors) {
            cursorQueue.remove(cursor);
            // an implementation aware cursor would probably be much faster and could skip whole blocks
            // see AdjacencyDecompressingReader#advance
            while (cursor.hasNextVLong() && cursor.peekVLong() < target) {
                cursor.nextVLong();
            }
            if (cursor.hasNextVLong()) {
                cursorQueue.add(cursor);
            }
        }

        return cursorQueue.isEmpty() ? AdjacencyCursor.NOT_FOUND : nextVLong();
    }

    @Override
    public long advanceBy(int n) {
        assert n >= 0;

        while (hasNextVLong()) {
            long target = nextVLong();
            if (n-- == 0) {
                return target;
            }
        }
        return NOT_FOUND;
    }

    @Override
    public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
        var destCursors = destination instanceof CompositeAdjacencyCursor
            ? ((CompositeAdjacencyCursor) destination).cursors
            : emptyList(cursors.size());

        var destIter = destCursors.listIterator();
        for (AdjacencyCursor cursor : cursors) {
            destIter.set(cursor.shallowCopy(destIter.next()));
        }
        return new CompositeAdjacencyCursor(destCursors);
    }

    private List<AdjacencyCursor> emptyList(int size) {
        return Arrays.asList(new AdjacencyCursor[size]);
    }

    @Override
    public void init(long index, int degree) {
        throw new UnsupportedOperationException(
            "CompositeAdjacencyCursor does not support init, use CompositeAdjacencyList.decompressingCursor instead.");
    }
}
