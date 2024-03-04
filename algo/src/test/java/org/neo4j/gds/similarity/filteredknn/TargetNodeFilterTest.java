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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TargetNodeFilterTest {
    @Test
    void shouldPrioritiseTargetNodes() {
        var consumer = ProvidedTargetNodeFilter.create(l -> true, 3, Optional.empty(), Double.MIN_VALUE);

        consumer.offer(23, 3.14);
        consumer.offer(42, 1.61);
        consumer.offer(87, 2.71);

        assertThat(consumer.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 87, 2.71),
            new SimilarityResult(117, 42, 1.61)
        );
    }

    @Test
    void shouldOnlyKeepTopK() {
        var consumer = ProvidedTargetNodeFilter.create(l -> true, 2, Optional.empty(), Double.MIN_VALUE);

        consumer.offer(23, 3.14);
        consumer.offer(42, 1.61);
        consumer.offer(87, 2.71);

        assertThat(consumer.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 87, 2.71)
        );
    }

    @Test
    void shouldOnlyIncludeTargetNodes() {
        var consumer = ProvidedTargetNodeFilter.create(l -> false, 3, Optional.empty(), Double.MIN_VALUE);

        consumer.offer(23, 3.14);
        consumer.offer(42, 1.61);
        consumer.offer(87, 2.71);

        assertThat(consumer.asSimilarityStream(117)).isEmpty();
    }

    @Test
    void shouldIgnoreExactDuplicates() {
        var consumer = ProvidedTargetNodeFilter.create(l -> true, 4, Optional.empty(), Double.MIN_VALUE);

        consumer.offer(23, 3.14);
        consumer.offer(42, 1.61);
        consumer.offer(87, 2.71);
        consumer.offer(42, 1.61);

        assertThat(consumer.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 87, 2.71),
            new SimilarityResult(117, 42, 1.61)
        );
    }

    /**
     * This is documenting a fact rather than illustrating something desirable.
     */
    @Test
    void shouldAllowDuplicateElementsWithNewPriorities() {
        var consumer = ProvidedTargetNodeFilter.create(l -> true, 4, Optional.empty(), Double.MIN_VALUE);

        consumer.offer(23, 3.14);
        consumer.offer(42, 1.61);
        consumer.offer(87, 2.71);
        consumer.offer(42, 1.41);

        assertThat(consumer.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 87, 2.71),
            new SimilarityResult(117, 42, 1.61),
            new SimilarityResult(117, 42, 1.41)
        );
    }

    @Test
    void shouldIgnoreLowScoringNodes() {
        var targetNodeFilter = ProvidedTargetNodeFilter.create(l -> true, 3, Optional.empty(), 2);

        targetNodeFilter.offer(23, 3.14);
        targetNodeFilter.offer(42, 1.61);
        targetNodeFilter.offer(87, 2.71);
        targetNodeFilter.offer(56, 1.41);

        assertThat(targetNodeFilter.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 87, 2.71)
        );
    }

    @Test
    void shouldNotIgnoreLowScoringNodesWhenSeeded() {
        Set<Pair<Double, Long>> seeds = Set.of(
            Pair.of(0.001, 256L),
            Pair.of(0.002, 512L),
            Pair.of(0.003, 384L)
        );

        var targetNodeFilter = ProvidedTargetNodeFilter.create(l -> true, 3, Optional.of(seeds), 2);

        targetNodeFilter.offer(23, 3.14);
        targetNodeFilter.offer(42, 1.61);

        assertThat(targetNodeFilter.asSimilarityStream(117)).containsExactly(
            new SimilarityResult(117, 23, 3.14),
            new SimilarityResult(117, 42, 1.61),
            new SimilarityResult(117, 384, 0.003)
        );
    }
}
