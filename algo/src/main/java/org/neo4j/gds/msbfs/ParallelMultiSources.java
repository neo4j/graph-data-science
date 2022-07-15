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
package org.neo4j.gds.msbfs;

import java.util.AbstractCollection;
import java.util.Iterator;

abstract class ParallelMultiSources extends AbstractCollection<MultiSourceBFSRunnable> implements Iterator<MultiSourceBFSRunnable> {
    private final int threads;
    private final long sourceLength;
    private long start = 0L;
    private int i = 0;

    ParallelMultiSources(int threads, long sourceLength) {
        this.threads = threads;
        this.sourceLength = sourceLength;
    }

    @Override
    public boolean hasNext() {
        return i < threads;
    }

    @Override
    public int size() {
        return threads;
    }

    @Override
    public Iterator<MultiSourceBFSRunnable> iterator() {
        start = 0L;
        i = 0;
        return this;
    }

    @Override
    public MultiSourceBFSRunnable next() {
        int len = (int) Math.min(MSBFSConstants.OMEGA, sourceLength - start);
        var bfs = next(start, len);
        start += len;
        i++;
        return bfs;
    }

    abstract MultiSourceBFSRunnable next(long from, int length);
}
