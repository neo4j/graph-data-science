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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.function.LongUnaryOperator;

public abstract class ReadOnlyHugeLongArrayAdapter extends HugeLongArray {

    @Override
    public void set(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long and(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void or(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addTo(long index, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAll(LongUnaryOperator gen) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long binarySearch(long searchValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeOf() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long release() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HugeCursor<long[]> newCursor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyTo(HugeLongArray dest, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int pages() {
        throw new UnsupportedOperationException();
    }
}
