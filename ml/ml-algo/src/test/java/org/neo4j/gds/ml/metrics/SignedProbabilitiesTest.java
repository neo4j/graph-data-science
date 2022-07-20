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
package org.neo4j.gds.ml.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.metrics.SignedProbabilities.ALMOST_ZERO;

final class SignedProbabilitiesTest {

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldEstimateCorrectly(long relationshipCount, double relationshipFraction) {
        var relevantRelationships = (long) (relationshipCount * relationshipFraction);
        long memory = SignedProbabilities.estimateMemory(relevantRelationships);

        var expected = MemoryUsage.sizeOfInstance(SignedProbabilities.class) +
                       MemoryUsage.sizeOfInstance(Optional.class) +
                       MemoryUsage.sizeOfInstance(ArrayList.class) +
                       relevantRelationships * (8 + 16);
        assertThat(memory).isEqualTo(expected);

    }

    @Test
    void shouldAddWithCorrectSignsAndReplaceZeroValues() {
        var signedProbabilities = SignedProbabilities.create(4);
        signedProbabilities.add(0.8, true);
        signedProbabilities.add(0.0, false);
        signedProbabilities.add(0.4, false);
        signedProbabilities.add(0.0, true);
        assertThat(signedProbabilities.stream().boxed().collect(Collectors.toList())).containsExactly(
            -ALMOST_ZERO,
            ALMOST_ZERO,
            -0.4,
            0.8
        );
    }

    static Stream<Arguments> parameters() {
        return LongStream.of(42, 1339).boxed().flatMap(relCount ->
            DoubleStream.of(0.0, 0.42, 1.0).mapToObj(relationshipFraction ->
                Arguments.of(relCount, relationshipFraction)
            )
        );
    }

    @Test
    void storeDuplicateProbabilities() {
        var signedProbabilities = SignedProbabilities.create(5);

        signedProbabilities.add(0.5, true);
        signedProbabilities.add(0.5, true);
        signedProbabilities.add(0.2, false);
        signedProbabilities.add(0.3, false);
        signedProbabilities.add(0.6, false);

        assertThat(signedProbabilities.positiveCount()).isEqualTo(2);
        assertThat(signedProbabilities.negativeCount()).isEqualTo(3);

        assertThat(signedProbabilities.stream()).containsExactly(-0.2, -0.3, 0.5, 0.5, -0.6);
    }

    @Test
    void hugeVersionStoresDuplicateProbabilities() {
        var signedProbabilities = new SignedProbabilities.Huge(5L);

        signedProbabilities.add(0.5, true);
        signedProbabilities.add(0.5, true);
        signedProbabilities.add(0.2, false);
        signedProbabilities.add(0.3, false);
        signedProbabilities.add(0.6, false);

        assertThat(signedProbabilities.positiveCount()).isEqualTo(2);
        assertThat(signedProbabilities.negativeCount()).isEqualTo(3);

        assertThat(signedProbabilities.stream()).containsExactly(-0.2, -0.3, 0.5, 0.5, -0.6);
    }
}
