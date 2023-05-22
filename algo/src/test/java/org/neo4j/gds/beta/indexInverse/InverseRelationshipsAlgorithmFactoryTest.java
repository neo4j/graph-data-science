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
package org.neo4j.gds.beta.indexInverse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class InverseRelationshipsAlgorithmFactoryTest {

    public static Stream<Arguments> compressions() {
        var compressedRunner = TestMethodRunner.runCompressedOrdered();
        var uncompressedRunner = TestMethodRunner.runUncompressedOrdered();

        return Stream.of(
            Arguments.of(compressedRunner, MemoryRange.of(1_462_320)),
            Arguments.of(uncompressedRunner, MemoryRange.of(2_248_808))
        );
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void memoryEstimationWithUncompressedFeatureToggle(TestMethodRunner runner, MemoryRange expected) {
        var factory = new InverseRelationshipsAlgorithmFactory();
        var config = InverseRelationshipsConfigImpl.builder().relationshipTypes("T1").build();

        GraphDimensions graphDimensions = GraphDimensions.of(100_000, 100_000);

        runner.run(() -> {
            var memoryEstimation = factory.memoryEstimation(config);

            assertThat(memoryEstimation
                .estimate(graphDimensions, config.concurrency())
                .memoryUsage()).isEqualTo(expected);
        });
    }

    @Test
    void memoryEstimationWithMultipleTypes() {
        var factory = new InverseRelationshipsAlgorithmFactory();
        var config = InverseRelationshipsConfigImpl.builder().relationshipTypes(List.of("T1", "T2")).build();

        GraphDimensions graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .build();

        var memoryEstimation = factory.memoryEstimation(config);

        MemoryTree memoryTree = memoryEstimation
            .estimate(graphDimensions, config.concurrency());

        assertThat(memoryTree.components().stream().map(MemoryTree::description)).containsExactly(
            "this.instance",
            "Inverse 'T1'",
            "Inverse 'T2'"
        );

        assertThat(memoryTree.memoryUsage()).isEqualTo(MemoryRange.of(2_924_608));
    }

    @Test
    void memoryEstimationWithTypeFilter() {
        var factory = new InverseRelationshipsAlgorithmFactory();
        var config = InverseRelationshipsConfigImpl.builder().relationshipTypes(List.of("T2")).build();

        GraphDimensions graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .build();

        var memoryEstimation = factory.memoryEstimation(config);

        MemoryTree memoryTree = memoryEstimation
            .estimate(graphDimensions, config.concurrency());

        assertThat(memoryTree.components().stream().map(MemoryTree::description)).containsExactly(
            "this.instance",
            "Inverse 'T2'"
        );
        var x = memoryTree.memoryUsage();

        assertThat(memoryTree.memoryUsage()).isEqualTo(MemoryRange.of(1_462_320));
    }

    @Test
    void memoryEstimationWithFilterStar() {
        var factory = new InverseRelationshipsAlgorithmFactory();
        var config = InverseRelationshipsConfigImpl.builder().relationshipTypes("*").build();

        GraphDimensions graphDimensions = GraphDimensions
            .builder()
            .nodeCount(100_000)
            .putRelationshipCount(RelationshipType.of("T1"), 10_000)
            .putRelationshipCount(RelationshipType.of("T2"), 90_000)
            .relCountUpperBound(100_000)
            .build();

        var memoryEstimation = factory.memoryEstimation(config);

        MemoryTree memoryTree = memoryEstimation
            .estimate(graphDimensions, config.concurrency());

        assertThat(memoryTree.components().stream().map(MemoryTree::description)).containsExactly(
            "this.instance",
            "Inverse '*'"
        );

        assertThat(memoryTree.memoryUsage()).isEqualTo(MemoryRange.of(2_924_608));
    }
}
