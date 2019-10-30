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

package org.neo4j.graphalgo.impl.jaccard;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.queue.LongPriorityQueue;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TopKMap implements Consumer<SimilarityResult> {

    static MemoryEstimation memoryEstimation(long items, int topk) {
        return MemoryEstimations.builder(TopKMap.class)
            .add("topk lists",
                MemoryEstimations.builder("topk lists", TopKList.class)
                    .add("queue", LongPriorityQueue.memoryEstimation(topk))
                    .build()
                    .times(items)
            )
            .build();
    }

    private final HugeObjectArray<TopKList> topKLists;

    TopKMap(long items, int topK, Comparator<SimilarityResult> comparator, AllocationTracker tracker) {
        topKLists = HugeObjectArray.newArray(TopKList.class, items, tracker);
        topKLists.setAll(node1 -> new TopKList(node1, topK, comparator));
    }

    @Override
    public void accept(SimilarityResult similarityResult) {
        topKLists.get(similarityResult.node1).accept(similarityResult);
    }

    public Stream<SimilarityResult> stream() {
        return LongStream.range(0, topKLists.size())
            .boxed()
            .flatMap(node1 -> topKLists.get(node1).stream());
    }

    // TODO: parallelize
    public long size() {
        long size = 0L;
        for (long i = 0; i < topKLists.size(); i++) {
            size += topKLists.get(i).similarityQueue.size();
        }
        return size;
    }

    public static final class TopKList implements Consumer<SimilarityResult> {

        private final long node1;
        private final LongPriorityQueue similarityQueue;
        private final int topK;
        private double minValue;
        private final PrimitiveDoubleComparator doubleComparator;

        TopKList(long node1, int topK, Comparator<SimilarityResult> comparator) {
            this.node1 = node1;
            this.topK = topK;

            if (comparator.equals(SimilarityResult.ASCENDING)) {
                this.doubleComparator = Double::compare;
                this.similarityQueue = LongPriorityQueue.min(topK);
            } else {
                this.doubleComparator = (d1, d2) -> Double.compare(d2, d1);
                this.similarityQueue = LongPriorityQueue.max(topK);
            }
        }

        @Override
        public void accept(SimilarityResult similarity) {
            if ((similarityQueue.size() < topK || doubleComparator.compare(similarity.similarity, minValue) < 0)) {
                similarityQueue.add(similarity.node2, similarity.similarity);
                minValue = similarityQueue.topCost();
            }
        }

        Stream<SimilarityResult> stream() {
            Stream.Builder<SimilarityResult> builder = Stream.builder();
            PrimitiveLongIterator iterator = similarityQueue.iterator();
            while (iterator.hasNext()) {
                long node2 = iterator.next();
                builder.add(new SimilarityResult(node1, node2, similarityQueue.getCost(node2)));
            }
            return builder.build();
        }
    }

    private interface PrimitiveDoubleComparator {
        int compare(double d1, double d2);
    }

}

