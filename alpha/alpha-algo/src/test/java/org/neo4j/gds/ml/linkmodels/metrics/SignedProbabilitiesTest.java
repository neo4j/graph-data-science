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
package org.neo4j.gds.ml.linkmodels.metrics;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.linkmodels.SignedProbabilities;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class SignedProbabilitiesTest {

    @ParameterizedTest
    @MethodSource("parameters")
    void shouldEstimateCorrectly(long nodeCount, long relationshipCount, double relationshipFraction) {

        var foo = RelationshipType.of("FOO");
        long memory = SignedProbabilities.estimateMemory(
            GraphDimensions
                .builder()
                .nodeCount(nodeCount)
                .relationshipCounts(Map.of(foo, relationshipCount))
                .build(),
            foo,
            relationshipFraction
        );

        var relevantRelationships = (long) (relationshipCount * relationshipFraction);
        var expected = MemoryUsage.sizeOfInstance(SignedProbabilities.class) +
                       MemoryUsage.sizeOfInstance(Optional.class) +
                       MemoryUsage.sizeOfInstance(ArrayList.class) +
                       relevantRelationships * (8 + 16);
        assertThat(memory).isEqualTo(expected);

    }

    static Stream<Arguments> parameters() {
        return LongStream.of(42, 1339).boxed().flatMap(
           nodeCount -> LongStream.of(100, 1000).boxed().flatMap(
               relCount -> DoubleStream.of(0.0, 0.42, 1.0).mapToObj(
                   relationshipFraction ->
                       Arguments.of(nodeCount, relCount, relationshipFraction)
               )
           )
        );
    }
}
