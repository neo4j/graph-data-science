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

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class TopKMap implements Consumer<SimilarityResult> {

    private final HugeObjectArray<TopKList> topKLists;

    TopKMap(long items, int topK, Comparator<SimilarityResult> comparator, AllocationTracker tracker) {
        topKLists = HugeObjectArray.newArray(TopKList.class, items, tracker);
        topKLists.setAll(i -> new TopKList(topK, comparator));
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

    public static final class TopKList implements Consumer<SimilarityResult> {

        private final SimilarityResult[] similarities;
        private final int topK;
        private int count = 0;
        private SimilarityResult minValue;
        private SimilarityResult maxValue;
        private final Comparator<SimilarityResult> comparator;

        TopKList(int topK, Comparator<SimilarityResult> comparator) {
            this.topK = topK;
            this.similarities = new SimilarityResult[topK];
            this.comparator = comparator;
        }

        @Override
        public void accept(SimilarityResult similarity) {
            if ((count < topK || minValue == null || comparator.compare(similarity, minValue) < 0)) {
                int idx = Arrays.binarySearch(similarities, 0, count, similarity, comparator);
                idx = (idx < 0) ? -idx : idx + 1;

                int length = topK - idx;
                if (length > 0 && idx < topK) System.arraycopy(similarities, idx - 1, similarities, idx, length);
                similarities[idx - 1] = similarity;
                if (count < topK) {
                    count++;
                }
                minValue = similarities[count - 1];
                maxValue = similarities[0];
            }
        }

        Stream<SimilarityResult> stream() {
            return Arrays.stream(similarities, 0, count);
        }
    }

}

