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
package org.neo4j.graphalgo.core.utils.paged;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.concurrent.CountedCompleter;
import java.util.concurrent.ForkJoinPool;

public final class HugeMergeSort {

    private static final int SEQUENTIAL_THRESHOLD = 100;

    public static void sort(HugeLongArray array, int concurrency, AllocationTracker tracker) {
        var temp = HugeLongArray.newArray(array.size(), tracker);

        var forkJoinPool = new ForkJoinPool(concurrency);

        var rootTask = new MergeSortTask(null, array, temp, 0, array.size() - 1);

        forkJoinPool.invoke(rootTask);
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
                this.midIndex = startIndex + endIndex >>> 1;

                var leftTask = new MergeSortTask(this, array, temp, startIndex, midIndex);
                var rightTask = new MergeSortTask(this, array, temp, midIndex + 1, endIndex);

                // 2 would make more sense, but 🤷
                setPendingCount(1);
                leftTask.fork();
                rightTask.fork();
            } else {
                superFastSequentialSort();
                tryComplete();
            }
        }

        private void superFastSequentialSort() {
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
                // j points to correct index, insert
                array.set(j + 1, current);
            }
        }

        @Override
        public void onCompletion(CountedCompleter<?> caller) {
            if (midIndex == 0) {
                // No merging for leaf tasks
                return;
            }

            // Copy range into temp
            for (long i = startIndex; i <= midIndex; i++) {
                temp.set(i, array.get(i));
            }

            long left = startIndex;
            long right = midIndex + 1;

            long i = startIndex;
            while (left <= midIndex && right <= endIndex) {
                if (temp.get(left) < array.get(right)) {
                    // if left is smaller, pick from left
                    array.set(i++, temp.get(left++));
                } else {
                    // if right is smaller, pick from right
                    array.set(i++, array.get(right++));
                }
            }

            if (left <= midIndex) {
                // Move remaining from temp into array
                for (long k = i; k <= endIndex; k++) {
                    array.set(k, temp.get(left++));
                }
            }
        }
    }

    private HugeMergeSort() {}
}
