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
package org.neo4j.gds.core.utils.partition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class LazyDegreePartitionIteratorTest {

    @ParameterizedTest
    @EnumSource(RelationshipDistribution.class)
    void testPartitionsComplete(RelationshipDistribution distribution) {
        var concurrency = 4;

        var graph = RandomGraphGenerator.builder()
            .nodeCount(10_000)
            .averageDegree(concurrency)
            .seed(42)
            .relationshipDistribution(distribution)
            .build()
            .generate();

        var partitions = LazyDegreePartitionIterator.of(
                graph.nodeCount(),
                graph.relationshipCount(),
                concurrency,
                graph::degree
            )
            .stream()
            .collect(Collectors.toList());

        assertThat(partitions.stream().mapToLong(DegreePartition::totalDegree).sum()).isEqualTo(graph.relationshipCount());

        for (int i = 0; i < partitions.size() - 1; i++) {
            DegreePartition partition = partitions.get(i);
            var nextPartition = partitions.get(i + 1);
            assertThat(partition.startNode() + partition.nodeCount()).isEqualTo(nextPartition.startNode());
        }
        DegreePartition lastPartition = partitions.get(partitions.size() - 1);
        assertThat(lastPartition.startNode() + lastPartition.nodeCount()).isEqualTo(graph.nodeCount());
    }

    @Test
    void testDegreePartitionWithMiddleHighDegreeNodes() {
        var nodeCount = 5;
        var degrees = new int[] { 1, 1, 10, 3, 1 };

        var partitions = LazyDegreePartitionIterator.of(
            nodeCount,
            Arrays.stream(degrees).sum(),
            4,
            idx -> degrees[(int) idx]
        );

        assertThat(partitions.stream()).containsExactly(
            DegreePartition.of(0, 2, 2),
            DegreePartition.of(2, 1, 10),
            DegreePartition.of(3, 1, 3),
            DegreePartition.of(4, 1, 1)
        );
    }
}
