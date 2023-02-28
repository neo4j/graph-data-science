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
package org.neo4j.gds.similarity.nodesim;

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.queue.BoundedLongLongPriorityQueue;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.PrimitiveIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TopNList {

    public static MemoryEstimation memoryEstimation(long nodeCount, int topN) {
        int actualTopN = topN;
        if (topN > nodeCount) { //avoid overflows for nodeCount * nodeCount (when nodeCount is >2^31)
            long normalizedMaximum = nodeCount * nodeCount;
            actualTopN = Math.toIntExact(Math.min(normalizedMaximum, topN));
        }
        return MemoryEstimations.builder(TopNList.class)
            .add("queue", BoundedLongLongPriorityQueue.memoryEstimation(actualTopN))
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
        Iterable<SimilarityResult> iterable = () -> new AbstractIterator<>() {

            final PrimitiveIterator.OfLong elements1Iter = queue.elements1().iterator();
            final PrimitiveIterator.OfLong elements2Iter = queue.elements2().iterator();
            final PrimitiveIterator.OfDouble prioritiesIter = queue.priorities().iterator();

            @Override
            protected SimilarityResult fetch() {
                if (!elements1Iter.hasNext() || !elements2Iter.hasNext() || !prioritiesIter.hasNext()) {
                    return done();
                }
                return new SimilarityResult(
                    elements1Iter.nextLong(),
                    elements2Iter.nextLong(),
                    prioritiesIter.nextDouble()
                );
            }
        };

        return StreamSupport.stream(iterable.spliterator(), true);
    }
}
