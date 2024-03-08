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

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

/**
 * This target node filter evaluates and stores incoming elements (neighbours) with their priority (score). We sort
 * elements by priority, descending. We respect similarity cutoff _unless_ a seed was supplied.
 *
 * For now it is a simple, bounded priority queue backed by a {@link java.util.TreeSet}. We handle duplicates, in the
 * sense that _exact_ pairs of element and priority, that already exist in the queue, are not added twice - this happens
 * to be the semantics of the {@link java.util.TreeSet} we use. So no duplicates in the output, even though our dear
 * {@link org.neo4j.gds.similarity.knn.Knn} algorithm does present us with such cases.
 *
 * NB: this data structure would _not_ handle "re-prioritisations" like a neighbour with a different score. Luckily we
 * have convinced ourselves that {@link org.neo4j.gds.similarity.knn.Knn} never presents us with such cases. So this
 * data structure suffices.
 */
public final class ExistingTargetNodeFilter implements TargetNodeFilter {
    private final TreeSet<Pair<Double, Long>> priorityQueue;
    private final LongPredicate predicate;
    private final int bound;
    private final boolean seeded;
    private final double similarityCutoff;

    /**
     * @param seeds Optionally seed the priority queue
     */
    static ExistingTargetNodeFilter create(
        LongPredicate targetNodePredicate,
        int bound,
        Optional<Set<Pair<Double, Long>>> seeds,
        double similarityCutoff
    ) {
        TreeSet<Pair<Double, Long>> priorityQueue = new TreeSet<>(Comparator.reverseOrder());

        seeds.ifPresent(priorityQueue::addAll);

        return new ExistingTargetNodeFilter(
            priorityQueue,
            seeds.isPresent(),
            similarityCutoff,
            targetNodePredicate,
            bound
        );
    }

    ExistingTargetNodeFilter(
        TreeSet<Pair<Double, Long>> priorityQueue,
        boolean seeded,
        double similarityCutoff,
        LongPredicate predicate,
        int bound
    ) {
        this.priorityQueue = priorityQueue;
        this.seeded = seeded;
        this.similarityCutoff = similarityCutoff;
        this.predicate = predicate;
        this.bound = bound;
    }

    @Override
    public void offer(long element, double priority) {
        if (!seeded && priority < similarityCutoff) return;

        if (!predicate.test(element)) return;

        priorityQueue.add(Pair.of(priority, element));

        if (priorityQueue.size() > bound) priorityQueue.pollLast();
    }

    /**
     * As part of an instrumentation of KNN this is a handy utility.
     */
    @Override
    public Stream<SimilarityResult> asSimilarityStream(long nodeId) {
        return priorityQueue.stream().map(p -> new SimilarityResult(nodeId, p.getRight(), p.getLeft()));
    }

    /**
     * As part of an instrumentation of KNN this is a handy utility.
     */
    @Override
    public long size() {
        return priorityQueue.size();
    }
}
