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
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.NeighbourConsumers;
import org.neo4j.gds.similarity.knn.SimilarityFunction;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.gds.similarity.filteredknn.EmptyTargetNodeFilter.EMPTY_CONSUMER;

public final class TargetNodeFiltering implements NeighbourConsumers {
    private final HugeObjectArray<TargetNodeFilter> targetNodeFilters;

    /**
     * @param optionalSimilarityFunction An actual similarity function if you want seeding, empty otherwise
     */

    private final boolean isFiltered;
    static TargetNodeFiltering create(
        NodeFilter sourceNodeFilter,
        long nodeCount,
        int k,
        LongPredicate targetNodePredicate,
        Optional<SimilarityFunction> optionalSimilarityFunction,
        double similarityCutoff,
        int concurrency
    ) {
        var neighbourConsumers = HugeObjectArray.newArray(TargetNodeFilter.class, nodeCount);
        boolean isFiltered = targetNodePredicate != NodeFilter.ALLOW_EVERYTHING;
        long[] startingSeeds = new long[k + 1];
        if (isFiltered) {
            findSeeds(
                startingSeeds,
                nodeCount,
                targetNodePredicate,
                k + 1,
                optionalSimilarityFunction.isEmpty()
            );
        }
        ParallelUtil.parallelForEachNode(nodeCount, concurrency, TerminationFlag.RUNNING_TRUE, (nodeId) -> {
            if (!sourceNodeFilter.test(nodeId) || !isFiltered) {
                neighbourConsumers.set(nodeId, EMPTY_CONSUMER);
            } else {
                var optionalPersonalizedSeeds = prepareNode(nodeId, startingSeeds, optionalSimilarityFunction);
                TargetNodeFilter targetNodeFilter = ProvidedTargetNodeFilter.create(
                    targetNodePredicate,
                    k,
                    optionalPersonalizedSeeds,
                    similarityCutoff
                );
                neighbourConsumers.set(nodeId, targetNodeFilter);
            }
        });


        return new TargetNodeFiltering(neighbourConsumers, isFiltered);

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
     */
    private static void findSeeds(
        long[] seeds,
        long nodeCount,
        LongPredicate targetNodePredicate,
        int k,
        boolean isEmpty
    ) {
        if (isEmpty) {
            return;
        }
        Arrays.fill(seeds, -1L);

        int index = 0;
        for (var nodeId = 0; (index < k && nodeId < nodeCount); ++nodeId) {
            if (targetNodePredicate.test(nodeId)) {
                seeds[index++] = nodeId;
            }

        }

    }

    //TODO: Replace the Pair<Double,Long>  stuff but not here not now
    private static Set<Pair<Double, Long>> prepareSeedSet(int k) {
        float defaultLoadFactor = 0.75f; // java.util.HashMap.DEFAULT_LOAD_FACTOR
        int initialCapacity = (int) (k / defaultLoadFactor); // see treatise in @HashMap JavaDoc
        return new HashSet<>(initialCapacity, defaultLoadFactor);
    }

    private static Optional<Set<Pair<Double, Long>>> prepareNode(
        long nodeId,
        long[] seeds,
        Optional<SimilarityFunction> optionalSimilarityFunction
    ) {

        return optionalSimilarityFunction.map(similarityFunction -> {
            var discoveredSeed = prepareSeedSet(seeds.length);
            for (int i = 0; i < seeds.length; ++i) {
                if (seeds[i] == -1) {
                    break;
                }
                if (seeds[i] == nodeId) {
                    continue;
                }
                discoveredSeed.add(Pair.of(similarityFunction.computeSimilarity(nodeId, seeds[i]), seeds[i]));
            }
            return Optional.of(discoveredSeed);
        }).orElse(Optional.empty());
    }


    private TargetNodeFiltering(HugeObjectArray<TargetNodeFilter> targetNodeFilters, boolean isFiltered) {
        this.targetNodeFilters = targetNodeFilters;
        this.isFiltered = isFiltered;
    }

    public boolean isFiltered() {
        return isFiltered;
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
