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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TopKConsumer<T> implements Function<T, Integer> {
    private final int topK;
    private final T[] heap;
    private Comparator<T> comparator;
    private int count;
    private T minValue;
    private T maxValue;

    public TopKConsumer(int topK, Comparator<T> comparator) {
        this.topK = topK;
        heap = (T[]) new Object[topK];
        this.comparator = comparator;
        count = 0;
        minValue = null;
        maxValue = null;
    }

    public static <T> List<T> topK(List<T> items, int topK, Comparator<T> comparator) {
        TopKConsumer<T> consumer = new TopKConsumer<>(topK, comparator);
        items.forEach(consumer::apply);
        return consumer.list();
    }

    public static <T> Stream<T> topK(Stream<T> items, int topK, Comparator<T> comparator) {
        TopKConsumer<T> consumer = new TopKConsumer<T>(topK, comparator);
        items.forEach(consumer::apply);
        return consumer.stream();
    }

    public static TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK, comparator);
        return results;
    }

    static SimilarityConsumer assignSimilarityPairs(TopKConsumer<SimilarityResult>[] topKConsumers) {
        return (s, t, result) -> {

            int selectedIndex = result.reversed ? t : s;
            topKConsumers[selectedIndex].apply(result);

            if (result.bidirectional) {
                SimilarityResult reverse = result.reverse();
                topKConsumers[reverse.reversed ? t : s].apply(reverse);
            }
        };
    }

    public Stream<T> stream() {
        return count< topK ? Arrays.stream(heap,0,count) : Arrays.stream(heap);
    }

    public List<T> list() {
        List<T> list = Arrays.asList(heap);
        return count< topK ? list.subList(0,count)  : list;
    }

    public int apply(TopKConsumer<T> other) {
        int changes = 0;
        if (minValue == null || count < topK || other.maxValue != null && comparator.compare(other.maxValue,minValue) < 0) {
            for (int i=0;i<other.count;i++) {
                changes += apply(other.heap[i]);
            }
        }
        return changes;
    }

    @Override
    public Integer apply(final T item) {
        if ((count < topK || minValue == null || comparator.compare(item,minValue) < 0)) {
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

}
