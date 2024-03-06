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
package org.neo4j.gds.indexInverse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.assertions.MemoryEstimationAssert;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.TestMethodRunner;

import java.util.List;
import java.util.stream.Stream;

class InverseRelationshipsMemoryEstimateDefinitionTest {

    private static Stream<Arguments> compressions() {
        var compressedRunner = TestMethodRunner.runCompressedOrdered();
        var uncompressedRunner = TestMethodRunner.runUncompressedOrdered();

        return Stream.of(
            Arguments.of(compressedRunner, 1_462_328),
            Arguments.of(uncompressedRunner, 2_248_816)
        );
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void memoryEstimationWithUncompressedFeatureToggle(TestMethodRunner runner, long expectedSize) {

        var graphDimensions = GraphDimensions.of(100_000, 100_000);

        runner.run(() -> {
            var memoryEstimation = new InverseRelationshipsMemoryEstimateDefinition(List.of("T1")).memoryEstimation();
            MemoryEstimationAssert.assertThat(memoryEstimation)
                .memoryTree(graphDimensions, 4)
                .memoryRange()
                .hasSameMinAndMaxEqualTo(expectedSize);
        });
    }

    @Test
    void memoryEstimationWithMultipleTypes() {
        var graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .relCountUpperBound(100_000)
            .build();

        var memoryEstimation = new InverseRelationshipsMemoryEstimateDefinition(List.of("T1", "T2")).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryTree(graphDimensions, 4)
            .componentsDescriptionsContainExactly(
                "this.instance",
                "Inverse 'T1'",
                "Inverse 'T2'"
            )
            .memoryRange()
            .hasSameMinAndMaxEqualTo(2_924_624);
    }

    @Test
    void memoryEstimationWithTypeFilter() {
        var graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .build();

        var memoryEstimation = new InverseRelationshipsMemoryEstimateDefinition(List.of("T2")).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryTree(graphDimensions, 4)
            .componentsDescriptionsContainExactly(
                "this.instance",
                "Inverse 'T2'"
            )
            .memoryRange()
            .hasSameMinAndMaxEqualTo(1_462_328);

    }

    @Test
    void memoryEstimationWithFilterStar() {
        var graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .relCountUpperBound(100_000)
            .build();

        var memoryEstimation = new InverseRelationshipsMemoryEstimateDefinition(List.of("*")).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryTree(graphDimensions, 4)
            .componentsDescriptionsContainExactly(
                "this.instance",
                "Inverse '*'"
            )
            .memoryRange()
            .hasSameMinAndMaxEqualTo(2_924_624);
    }
}
