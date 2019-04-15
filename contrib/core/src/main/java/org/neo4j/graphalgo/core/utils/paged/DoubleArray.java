/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.paged;

import org.neo4j.graphalgo.core.write.PropertyTranslator;

import java.util.Arrays;

public final class DoubleArray extends PagedDataStructure<double[]> {

    private static final PageAllocator.Factory<double[]> ALLOCATOR_FACTORY =
            PageAllocator.ofArray(double[].class);

    public static long estimateMemoryUsage(long size) {
        return ALLOCATOR_FACTORY.estimateMemoryUsage(size, DoubleArray.class);
    }

    public static DoubleArray newArray(long size, AllocationTracker tracker) {
        return new DoubleArray(size, ALLOCATOR_FACTORY.newAllocator(tracker));
    }

    private DoubleArray(long size, PageAllocator<double[]> allocator) {
        super(size, allocator);
    }

    public double get(long index) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        return pages[pageIndex][indexInPage];
    }

    public double set(long index, double value) {
        assert index < capacity();
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        final double[] page = pages[pageIndex];
        final double ret = page[indexInPage];
        page[indexInPage] = value;
        return ret;
    }

    public void add(long index, double value) {
        final int pageIndex = pageIndex(index);
        final int indexInPage = indexInPage(index);
        pages[pageIndex][indexInPage] += value;
    }

    public void fill(double value) {
        for (double[] page : pages) {
            Arrays.fill(page, value);
        }
    }

    public static class Translator implements PropertyTranslator.OfDouble<DoubleArray> {

        public static final PropertyTranslator<DoubleArray> INSTANCE = new Translator();

        @Override
        public double toDouble(final DoubleArray data, final long nodeId) {
            return data.get(nodeId);
        }
    }

}
