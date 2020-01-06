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
package org.neo4j.graphalgo.impl.nodesim;

import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.queue.BoundedLongLongPriorityQueue;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TopNList {

    static MemoryEstimation memoryEstimation(int topN) {
        return MemoryEstimations.builder(TopNList.class)
            .add("queue", BoundedLongLongPriorityQueue.memoryEstimation(topN))
            .build();
    }

    private final BoundedLongLongPriorityQueue queue;

    public TopNList(int top) {
        int absTop = Math.abs(top);

        this.queue = top > 0
            ? BoundedLongLongPriorityQueue.max(absTop)
            : BoundedLongLongPriorityQueue.min(absTop);
    }

    public void add(long node1, long node2, double similarity) {
        queue.offer(node1, node2, similarity);
    }

    public Stream<SimilarityResult> stream() {
        Iterable<SimilarityResult> iterable = () -> new Iterator<SimilarityResult>() {

            PrimitiveIterator.OfLong elements1Iter = queue.elements1().iterator();
            PrimitiveIterator.OfLong elements2Iter = queue.elements2().iterator();
            PrimitiveIterator.OfDouble prioritiesIter = queue.priorities().iterator();

            @Override
            public boolean hasNext() {
                return elements1Iter.hasNext();
            }

            @Override
            public SimilarityResult next() {
                return new SimilarityResult(elements1Iter.nextLong(), elements2Iter.nextLong(), prioritiesIter.nextDouble());
            }
        };

        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
