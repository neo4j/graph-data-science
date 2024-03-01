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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphAdapter;
import org.neo4j.gds.similarity.SimilarityResult;
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
        TargetNodeFiltering targetNodeFiltering = TargetNodeFiltering.create(
            thereIsSixNodeInThisFilter,
            kIsTwo,
            evenNodesAreTargetNodes,
            null /* not needed when not seeding */,
            Optional.empty(),  /* not needed when not seeding */
            noSimilarityCutoff,
            1
        );

        TargetNodeFilter targetNodeFilter = targetNodeFiltering.get(0);
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
        Graph graphWithAtLeastKNodes = new GraphAdapter(null) {
            @Override
            public Graph concurrentCopy() {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            public void forEachNode(LongPredicate consumer) {
                int i = 0;
                while (true) {
                    if (!consumer.test(i++)) break;
                }
            }
        };
        Optional<SimilarityFunction> weSeedWithLowScores = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 2.71;
            }
        });
        double noSimilarityCutoff = Double.MIN_VALUE;
        TargetNodeFiltering targetNodeFiltering = TargetNodeFiltering.create(
            thereIsSixNodeInThisFilter,
            kIsFive,
            allNodesAreTargetNodes,
            graphWithAtLeastKNodes,
            weSeedWithLowScores,
            noSimilarityCutoff,
            1
        );

        // we only offer three high quality nodes
        TargetNodeFilter targetNodeFilter = targetNodeFiltering.get(0);
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
        Graph graphWithAtLeastKNodes = new GraphAdapter(null) {
            @Override
            public Graph concurrentCopy() {
                throw new UnsupportedOperationException("TODO");
            }

            @Override
            public void forEachNode(LongPredicate consumer) {
                int i = 0;
                while (true) {
                    if (!consumer.test(i++)) break;
                }
            }
        };
        Optional<SimilarityFunction> weSeedWithLowScores = Optional.of(new SimilarityFunction(null) {
            @Override
            public double computeSimilarity(long n, long m) {
                return 2.71;
            }
        });
        double noSimilarityCutoff = Double.MIN_VALUE;
        TargetNodeFiltering targetNodeFiltering = TargetNodeFiltering.create(
            thereIsSixNodeInThisFilter,
            kIsFive,
            allNodesAreTargetNodes,
            graphWithAtLeastKNodes,
            weSeedWithLowScores,
            noSimilarityCutoff,
            1
        );

        // this is only the seeds
        Stream<SimilarityResult> similarityResultStream = targetNodeFiltering.get(0).asSimilarityStream(0);

        assertThat(similarityResultStream.mapToLong(SimilarityResult::targetNodeId)).doesNotContain(0L);
    }
}
