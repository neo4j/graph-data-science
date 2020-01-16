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
package org.neo4j.graphalgo.nodesim;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.core.utils.SetBitsIterable;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongLongPriorityQueue;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongPriorityQueue;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TopKMap {

    private final BitSet nodeFilter;

    static MemoryEstimation memoryEstimation(long nodes, int topK) {
        return MemoryEstimations.builder(TopKMap.class)
            .add("topK lists",
                MemoryEstimations.builder("topK lists", TopKList.class)
                    .add("queues", BoundedLongPriorityQueue.memoryEstimation(topK))
                    .build()
                    .times(nodes)
            )
            .build();
    }

    private final HugeObjectArray<TopKList> topKLists;

    TopKMap(
        long items,
        BitSet nodeFilter,
        int topK,
        Comparator<SimilarityResult> comparator,
        AllocationTracker tracker
    ) {
        this.nodeFilter = nodeFilter;
        int boundedTopK = (int) Math.min(topK, items);
        topKLists = HugeObjectArray.newArray(TopKList.class, items, tracker);
        topKLists.setAll(node1 -> nodeFilter.get(node1)
            ? new TopKList(comparator.equals(SimilarityResult.ASCENDING)
                ? BoundedLongPriorityQueue.min(boundedTopK)
                : BoundedLongPriorityQueue.max(boundedTopK)
            ) : null
        );
    }

    public void put(long node1, long node2, double similarity) {
        topKLists.get(node1).accept(node2, similarity);
    }

    public TopKList get(long node1) {
        return topKLists.get(node1);
    }

    long similarityPairCount() {
        SetBitsIterable longs = new SetBitsIterable(nodeFilter);
        PrimitiveIterator.OfLong iterator = longs.iterator();

        long size = 0L;
        while (iterator.hasNext()) {
            size += topKLists.get(iterator.next()).size();
        }

        return size;
    }

    public void forEach(BoundedLongLongPriorityQueue.Consumer consumer) {
        SetBitsIterable items = new SetBitsIterable(nodeFilter);
        items.stream().forEach(element1 -> {
            BoundedLongPriorityQueue queue = topKLists.get(element1).queue;
            PrimitiveIterator.OfLong node2Iterator = queue.elements().iterator();
            PrimitiveIterator.OfDouble priorityIterator = queue.priorities().iterator();
            while (node2Iterator.hasNext()) {
                consumer.accept(element1, node2Iterator.nextLong(), priorityIterator.nextDouble());
            }
        });
    }

    public Stream<SimilarityResult> stream() {
        return new SetBitsIterable(nodeFilter).stream()
            .boxed()
            .flatMap(node1 -> topKLists.get(node1).stream(node1));
    }

    public static final class TopKList {

        private final BoundedLongPriorityQueue queue;

        TopKList(BoundedLongPriorityQueue queue) {
            this.queue = queue;
        }

        int size() {
            return queue.size();
        }

        void accept(long node2, double similarity) {
            queue.offer(node2, similarity);
        }

        void forEach(BoundedLongPriorityQueue.Consumer consumer) {
            queue.forEach(consumer);
        }

        Stream<SimilarityResult> stream(long node1) {

            Iterable<SimilarityResult> iterable = () -> new Iterator<SimilarityResult>() {

                PrimitiveIterator.OfLong elementsIter = queue.elements().iterator();
                PrimitiveIterator.OfDouble prioritiesIter = queue.priorities().iterator();

                @Override
                public boolean hasNext() {
                    return elementsIter.hasNext() && prioritiesIter.hasNext();
                }

                @Override
                public SimilarityResult next() {
                    return new SimilarityResult(node1, elementsIter.nextLong(), prioritiesIter.nextDouble());
                }
            };

            return StreamSupport.stream(iterable.spliterator(), false);
        }
    }
}

