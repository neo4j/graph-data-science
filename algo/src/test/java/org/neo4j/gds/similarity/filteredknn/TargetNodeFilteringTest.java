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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.SimilarityFunction;

import java.util.Optional;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class TargetNodeFilteringTest {
    @Test
    void shouldFilterTargetNodes() {
        int thereIsSixNodeInThisFilter = 6;
        int kIsTwo = 2;
        LongPredicate evenNodesAreTargetNodes = l -> l % 2 == 0;
        double noSimilarityCutoff = Double.MIN_VALUE;
        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            NodeFilter.ALLOW_EVERYTHING,
            thereIsSixNodeInThisFilter,
            kIsTwo,
            evenNodesAreTargetNodes,
            Optional.empty(),  /* not needed when not seeding */
            noSimilarityCutoff,
            1
        );

        var targetNodeFilter = targetNodeFiltering.get(0);
        targetNodeFilter.offer(40, 3.14);
        targetNodeFilter.offer(41, 3.15);
        targetNodeFilter.offer(42, 3.16);
        targetNodeFilter.offer(43, 3.17);
        targetNodeFilter.offer(44, 3.18);
        targetNodeFilter.offer(45, 3.19);

        assertThat(targetNodeFilter.asSimilarityStream(0)).containsExactly(
            new SimilarityResult(0, 44, 3.18),
            new SimilarityResult(0, 42, 3.16)
        );
    }

    @Test
    void shouldSeed() {
        int thereIsSixNodeInThisFilter = 6;
        int kIsFive = 5;
        LongPredicate allNodesAreTargetNodes = l -> true;

        Optional<SimilarityFunction> weSeedWithLowScores = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 2.71;
            }
        });
        double noSimilarityCutoff = Double.MIN_VALUE;
        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            NodeFilter.ALLOW_EVERYTHING,
            thereIsSixNodeInThisFilter,
            kIsFive,
            allNodesAreTargetNodes,
            weSeedWithLowScores,
            noSimilarityCutoff,
            1
        );

        // we only offer three high quality nodes
        var targetNodeFilter = targetNodeFiltering.get(0);
        targetNodeFilter.offer(40, 3.14);
        targetNodeFilter.offer(41, 3.15);
        targetNodeFilter.offer(42, 3.16);

        // but we guarantee outputting five!
        assertThat(targetNodeFilter.asSimilarityStream(0)).containsExactly(
            new SimilarityResult(0, 42, 3.16),
            new SimilarityResult(0, 41, 3.15),
            new SimilarityResult(0, 40, 3.14),
            new SimilarityResult(0, 5, 2.71),
            new SimilarityResult(0, 4, 2.71)
        );
    }

    @Test
    void shouldNotSeedWithSelf() {
        int thereIsSixNodeInThisFilter = 6;
        int kIsFive = 5;
        LongPredicate allNodesAreTargetNodes = l -> true;

        Optional<SimilarityFunction> weSeedWithLowScores = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 2.71;
            }
        });
        double noSimilarityCutoff = Double.MIN_VALUE;
        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            NodeFilter.ALLOW_EVERYTHING,
            thereIsSixNodeInThisFilter,
            kIsFive,
            allNodesAreTargetNodes,
            weSeedWithLowScores,
            noSimilarityCutoff,
            1
        );

        // this is only the seeds
        Stream<SimilarityResult> similarityResultStream = targetNodeFiltering.get(0).asSimilarityStream(0);

        assertThat(similarityResultStream.mapToLong(SimilarityResult::targetNodeId)).doesNotContain(0L);
    }

    @Test
    void shouldRestrictToSourceFilteredNodes() {
        int thereIsSixNodeInThisFilter = 6;
        int kIsTwo = 2;
        LongPredicate evenNodesAreTargetNodes = l -> l % 2 == 0;
        double noSimilarityCutoff = Double.MIN_VALUE;
        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            l -> l > 0,
            thereIsSixNodeInThisFilter,
            kIsTwo,
            evenNodesAreTargetNodes,
            Optional.empty(),  /* not needed when not seeding */
            noSimilarityCutoff,
            1
        );

        var targetNodeFilterZero = targetNodeFiltering.get(0);
        var targetNodeFilterOne = targetNodeFiltering.get(1);
        assertThat(targetNodeFilterZero).isInstanceOf(EmptyTargetNodeFilter.class);
        assertThat(targetNodeFilterOne).isInstanceOf(ProvidedTargetNodeFilter.class);

    }

    @Test()
    void shouldRealizeThereAreNotEnoughTargetNodes() {
        int gazillionNodes = 100;
        int gazillionK = 10;
        LongPredicate targetNodes = l -> (l >= 2 && l <= 5);
        double noSimilarityCutoff = Double.MIN_VALUE;

        Optional<SimilarityFunction> weSeed = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 1.337 + 0.42;
            }
        });

        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            NodeFilter.ALLOW_EVERYTHING,
            gazillionNodes,
            gazillionK,
            targetNodes,
            weSeed,
            noSimilarityCutoff,
            1
        );

        assertThat(targetNodeFiltering.seedingSummary().seededOptimally()).isTrue();

    }

    @Test()
    void shouldClaimNotSeedOptimallyOtherwise() {
        int gazillionNodes = 100;
        int gazillionK = 3;
        LongPredicate targetNodes = l -> (l >= 2 && l <= 5);
        double noSimilarityCutoff = Double.MIN_VALUE;

        Optional<SimilarityFunction> weSeed = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 1.337 + 0.42;
            }
        });

        ProvidedTargetNodeFiltering targetNodeFiltering = ProvidedTargetNodeFiltering.create(
            NodeFilter.ALLOW_EVERYTHING,
            gazillionNodes,
            gazillionK,
            targetNodes,
            weSeed,
            noSimilarityCutoff,
            1
        );

        assertThat(targetNodeFiltering.seedingSummary().seededOptimally()).isFalse();

    }
}
