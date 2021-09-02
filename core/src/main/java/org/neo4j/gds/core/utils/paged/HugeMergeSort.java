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
package org.neo4j.gds.core.utils.paged;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.concurrent.CountedCompleter;

public final class HugeMergeSort {

    private static final int SEQUENTIAL_THRESHOLD = 100;

    public static void sort(HugeLongArray array, int concurrency, AllocationTracker allocationTracker) {
        var temp = HugeLongArray.newArray(array.size(), allocationTracker);
        var forkJoinPool = ParallelUtil.getFJPoolWithConcurrency(concurrency);
        forkJoinPool.invoke(new MergeSortTask(null, array, temp, 0, array.size() - 1));
    }

    static class MergeSortTask extends CountedCompleter<Void> {

        private final HugeLongArray array;
        private final HugeLongArray temp;

        private final long startIndex;
        private final long endIndex;

        private long midIndex;

        MergeSortTask(
            @Nullable CountedCompleter<?> completer,
            HugeLongArray array,
            HugeLongArray temp,
            long startIndex,
            long endIndex
        ) {
            super(completer);
            this.array = array;
            this.temp = temp;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void compute() {
            if (endIndex - startIndex >= SEQUENTIAL_THRESHOLD) {
                // We split the range in half and spawn two
                // new sub task for left and right range.
                this.midIndex = startIndex + endIndex >>> 1;

                var leftTask = new MergeSortTask(this, array, temp, startIndex, midIndex);
                var rightTask = new MergeSortTask(this, array, temp, midIndex + 1, endIndex);

                // 2 would make more sense, but 🤷
                setPendingCount(1);
                leftTask.fork();
                rightTask.fork();
            } else {
                // We sort the range sequentially before
                // propagating "done" to the "completer".
                insertionSort(array, startIndex, endIndex);
                // This calls into "onCompletion" which
                // performs the merge of the two sub-ranges
                // and decrements the pending count.
                tryComplete();
            }
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            if (midIndex == 0) {
                // No merging for leaf tasks.
                return;
            }
            merge(array, temp, startIndex, endIndex, midIndex);
        }
    }

    private static void merge(HugeLongArray array, HugeLongArray temp, long startIndex, long endIndex, long midIndex) {
        // Copy only left range into temp
        for (long i = startIndex; i <= midIndex; i++) {
            temp.set(i, array.get(i));
        }

        // Left points to the next element in the left range.
        long left = startIndex;
        // Right points to the next element in the right range.
        long right = midIndex + 1;

        // i points to the next element in the full range.
        long i = startIndex;
        while (left <= midIndex && right <= endIndex) {
            // Each iteration inserts an element into array
            // at position i. We take the smaller element from
            // either left or right range and increment the
            // corresponding range index.
            if (temp.get(left) < array.get(right)) {
                array.set(i++, temp.get(left++));
            } else {
                array.set(i++, array.get(right++));
            }
        }

        // If we still have elements in the temp range, we need
        // to move them at the end of the range since we know
        // that all values in the right range are smaller.
        if (left <= midIndex) {
            for (long k = i; k <= endIndex; k++) {
                array.set(k, temp.get(left++));
            }
        }
    }

    private static void insertionSort(HugeLongArray array, long startIndex, long endIndex) {
        for (long i = startIndex, j = i; i < endIndex; j = ++i) {
            // Try to find a spot for current
            long current = array.get(i + 1);

            // Copy values greater than `current` to the right
            while (current < array.get(j)) {
                array.set(j + 1, array.get(j));

                if (j-- == startIndex) {
                    break;
                }
            }

            // We found the right position for "current".
            array.set(j + 1, current);
        }
    }

    private HugeMergeSort() {}
}
