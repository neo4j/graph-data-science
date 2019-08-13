/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.AbstractIterator;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class LazyBatchCollection<T> extends AbstractCollection<T> {

    public interface BatchSupplier<T> {
        T newBatch(long start, long length);
    }

    public static <T> Collection<T> of(
            long nodeCount,
            long batchSize,
            BatchSupplier<T> supplier) {
        return new LazyBatchCollection<>(batchSize, nodeCount, false, supplier);
    }

    public static <T> Collection<T> ofCached(
            long nodeCount,
            long batchSize,
            BatchSupplier<T> supplier) {
        return new LazyBatchCollection<>(batchSize, nodeCount, true, supplier);
    }

    private final boolean saveResults;
    private final BatchSupplier<T> supplier;
    private final long nodeCount;
    private final long batchSize;
    private final int numberOfBatches;

    private List<T> batches;

    private LazyBatchCollection(
            long batchSize,
            long nodeCount,
            boolean saveResults,
            BatchSupplier<T> supplier) {
        this.saveResults = saveResults;
        this.supplier = supplier;
        this.nodeCount = nodeCount;
        this.batchSize = batchSize;
        numberOfBatches = Math.toIntExact(ParallelUtil.threadCount(batchSize, nodeCount));
    }

    @Override
    public final Iterator<T> iterator() {
        if (batches != null) {
            return batches.iterator();
        }
        if (saveResults) {
            batches = new ArrayList<>(numberOfBatches);
        }
        return new AbstractIterator<T>() {
            private int i;
            private long start;

            @Override
            protected T fetch() {
                int i = this.i++;
                if (i >= numberOfBatches) {
                    return done();
                }
                long start = this.start;
                this.start += batchSize;
                long length = Math.min(batchSize, nodeCount - start);
                T t = supplier.newBatch(start, length);
                if (batches != null) {
                    batches.add(t);
                }
                return t;
            }
        };
    }

    @Override
    public final int size() {
        if (batches != null) {
            return batches.size();
        }
        return numberOfBatches;
    }
}
