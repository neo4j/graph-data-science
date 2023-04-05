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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.NeighbourConsumers;
import org.neo4j.gds.similarity.knn.SimilarityFunction;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class TargetNodeFiltering implements NeighbourConsumers {
    private final HugeObjectArray<TargetNodeFilter> targetNodeFilters;

    /**
     * @param optionalSimilarityFunction An actual similarity function if you want seeding, empty otherwise
     */
    static TargetNodeFiltering create(
        long nodeCount,
        int k,
        LongPredicate targetNodePredicate,
        Graph graph,
        Optional<SimilarityFunction> optionalSimilarityFunction,
        double similarityCutoff
    ) {
        var neighbourConsumers = HugeObjectArray.newArray(TargetNodeFilter.class, nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            var optionalSeeds = prepareSeeds(graph, targetNodePredicate, k, i, optionalSimilarityFunction);
            TargetNodeFilter targetNodeFilter = TargetNodeFilter.create(targetNodePredicate, k, optionalSeeds, similarityCutoff);

            neighbourConsumers.set(i, targetNodeFilter);
        }

        return new TargetNodeFiltering(neighbourConsumers);
    }

    /**
     * There are 17 ways to do seeding, this is clearly the dumbest one. It gets us a walking skeleton and let's us
     * design UI etc. You would describe this as, seed every target node set with the first k target nodes encountered
     * during a scan.
     *
     * A plus feature is, we fill up with k target nodes iff k target nodes can be found. This is of course unsolvable.
     *
     * Cons include bias for start of node array, yada yada. Extremely naive solution at this point.
     *
     * @param similarityFunction An actual similarity function if you want seeds, empty otherwise.
     */
    private static Optional<Set<Pair<Double, Long>>> prepareSeeds(
        Graph graph,
        LongPredicate targetNodePredicate,
        int k,
        int n,
        Optional<SimilarityFunction> similarityFunction
    ) {
        if (similarityFunction.isEmpty()) { return Optional.empty(); }

        Set<Pair<Double, Long>> seeds = prepareSeedSet(k);

        graph.forEachNode(m -> {
            if (n == m) return true;

            if (!targetNodePredicate.test(m)) return true;

            double similarityScore = similarityFunction.get().computeSimilarity(n, m);

            seeds.add(Pair.of(similarityScore, m));

            return seeds.size() < k;
        });

        return Optional.of(seeds);
    }

    /**
     * Ensuring the @{@link java.util.HashSet} never needs to resize
     */
    @NotNull
    private static Set<Pair<Double, Long>> prepareSeedSet(int k) {
        float defaultLoadFactor = 0.75f; // java.util.HashMap.DEFAULT_LOAD_FACTOR
        int initialCapacity = (int) (k / defaultLoadFactor); // see treatise in @HashMap JavaDoc
        return new HashSet<>(initialCapacity, defaultLoadFactor);
    }

    private TargetNodeFiltering(HugeObjectArray<TargetNodeFilter> targetNodeFilters) {
        this.targetNodeFilters = targetNodeFilters;
    }

    @Override
    public TargetNodeFilter get(long nodeId) {
        return targetNodeFilters.get(nodeId);
    }

    Stream<SimilarityResult> asSimilarityResultStream(LongPredicate sourceNodePredicate) {
        return Stream
            .iterate(
                targetNodeFilters.initCursor(targetNodeFilters.newCursor()),
                HugeCursor::next,
                UnaryOperator.identity()
            )
            .flatMap(cursor -> IntStream.range(cursor.offset, cursor.limit)
                .filter(index -> sourceNodePredicate.test(index + cursor.base))
                .mapToObj(index -> cursor.array[index].asSimilarityStream(index + cursor.base))
                .flatMap(Function.identity())
            );
    }

    long numberOfSimilarityPairs(LongPredicate sourceNodePredicate) {
        return Stream
            .iterate(
                targetNodeFilters.initCursor(targetNodeFilters.newCursor()),
                HugeCursor::next,
                UnaryOperator.identity()
            )
            .flatMapToLong(cursor -> IntStream.range(cursor.offset, cursor.limit)
                .filter(index -> sourceNodePredicate.test(index + cursor.base))
                .mapToLong(index -> cursor.array[index].size())
            ).sum();
    }
}
