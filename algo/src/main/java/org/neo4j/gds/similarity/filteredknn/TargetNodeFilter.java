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
package org.neo4j.gds.similarity.filteredknn;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.NeighbourConsumer;

import java.util.Comparator;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * We sort results by score, descending.
 *
 * For now a simple bounded priority queue that does _not_ handle duplicates.
 */
public class TargetNodeFilter implements NeighbourConsumer {
    private final TreeSet<Pair<Double, Long>> priorityQueue = new TreeSet<>(Comparator.reverseOrder());
    private final int bound;

    public TargetNodeFilter(int bound) {
        this.bound = bound;
    }

    @Override
    public void offer(long element, double priority) {
        priorityQueue.add(Pair.of(priority, element));

        if (priorityQueue.size() > bound) priorityQueue.pollLast();
    }

    /**
     * As part of an instrumentation of KNN this is a handy utility.
     */
    Stream<SimilarityResult> asSimilarityStream(long nodeId) {
        return priorityQueue.stream().map(p -> new SimilarityResult(nodeId, p.getRight(), p.getLeft()));
    }
}
