/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.impl.results.SimilarityResult;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public class AnnTopKConsumer implements ToIntFunction<SimilarityResult> {
    private final int topK;
    private final SimilarityResult[] heap;
    private final Comparator<SimilarityResult> comparator;
    private int count;
    private SimilarityResult minValue;
    private SimilarityResult maxValue;

    public AnnTopKConsumer(int topK, Comparator<SimilarityResult> comparator) {
        this.topK = topK;
        this.heap = new SimilarityResult[topK];
        this.comparator = comparator;
        count = 0;
        minValue = null;
        maxValue = null;
    }

    public static AnnTopKConsumer[] initializeTopKConsumers(int length, final int initialTopK) {
        int topK = initialTopK;
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        AnnTopKConsumer[] results = new AnnTopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new AnnTopKConsumer(topK, comparator);
        return results;
    }

    public Stream<SimilarityResult> stream() {
        return count< topK ? Arrays.stream(heap,0,count) : Arrays.stream(heap);
    }

    public int apply(AnnTopKConsumer other) {
        int changes = 0;
        if (minValue == null || count < topK || other.maxValue != null && comparator.compare(other.maxValue,minValue) < 0) {
            for (int i=0;i<other.count;i++) {
                changes += applyAsInt(other.heap[i]);
            }
        }
        return changes;
    }

    @Override
    public int applyAsInt(SimilarityResult item) {
        if ((count < topK || minValue == null || comparator.compare(item,minValue) < 0)) {
            if(heapAlreadyHasItem(item)) {
                return 0;
            }

            int idx = Arrays.binarySearch(heap, 0, count, item, comparator);
            idx = (idx < 0) ? -idx : idx + 1;

            int length = topK - idx;
            if (length > 0 && idx < topK) System.arraycopy(heap,idx-1,heap,idx, length);
            heap[idx-1]=item;
            if (count< topK) count++;
            minValue = heap[count-1];
            maxValue = heap[0];
            return 1;
        }
        return 0;
    }

    private boolean heapAlreadyHasItem(final SimilarityResult item) {
        for (int i = 0; i < count; i++) {
            if(heap[i].sameItems(item)) {
                return true;
            }

        }

        return false;
    }

    @Override
    public String toString() {
        return "AnnTopKConsumer{" +
               "heap=" + Arrays.toString(heap) +
               '}';
    }
}
