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
package org.neo4j.graphalgo.similarity;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TopKConsumer<T> implements Consumer<T> {
    private final int topK;
    private final T[] heap;
    private Comparator<T> comparator;
    private int count;
    private T minValue;

    public TopKConsumer(int topK, Comparator<T> comparator) {
        this.topK = topK;
        heap = (T[]) new Object[topK];
        this.comparator = comparator;
        count = 0;
        minValue = null;
    }

    public static <T> List<T> topK(List<T> items, int topK, Comparator<T> comparator) {
        TopKConsumer<T> consumer = new TopKConsumer<>(topK, comparator);
        items.forEach(consumer);
        return consumer.list();
    }

    public static <T> Stream<T> topK(Stream<T> items, int topK, Comparator<T> comparator) {
        TopKConsumer<T> consumer = new TopKConsumer<T>(topK, comparator);
        items.forEach(consumer);
        return consumer.stream();
    }

    static TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK, comparator);
        return results;
    }

    static SimilarityConsumer assignSimilarityPairs(TopKConsumer<SimilarityResult>[] topKConsumers) {
        return (s, t, result) -> {

            int selectedIndex = result.reversed ? t : s;
            topKConsumers[selectedIndex].accept(result);

            if (result.bidirectional) {
                SimilarityResult reverse = result.reverse();
                topKConsumers[reverse.reversed ? t : s].accept(reverse);
            }
        };
    }

    @Override
    public void accept(T item) {
        if (count < topK || minValue == null || comparator.compare(item,minValue) < 0) {
            int idx = Arrays.binarySearch(heap, 0, count, item, comparator);
            idx = (idx < 0) ? -idx : idx + 1;
            int length = topK - idx;
            if (length > 0 && idx < topK) System.arraycopy(heap,idx-1,heap,idx, length);
            heap[idx-1]=item;
            if (count< topK) count++;
            minValue = heap[count-1];
        }
    }

    public Stream<T> stream() {
        return count< topK ? Arrays.stream(heap,0,count) : Arrays.stream(heap);
    }

    public List<T> list() {
        List<T> list = Arrays.asList(heap);
        return count< topK ? list.subList(0,count)  : list;
    }

    public void accept(TopKConsumer<T> other) {
        if (minValue == null || count < topK || other.minValue != null && comparator.compare(other.minValue,minValue) < 0) {
            for (int i=0;i<other.count;i++) {
                accept(other.heap[i]);
            }
        }
    }
}
