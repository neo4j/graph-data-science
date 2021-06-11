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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.eclipse.collections.impl.block.factory.Functions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class PartitionUtilsTest {

    @Test
    void testAlignment() {
        long alignTo = 64;
        long nodeCount = 200;
        int concurrency = 2;

        List<Partition> partitions = PartitionUtils.numberAlignedPartitioning(
            concurrency,
            nodeCount,
            alignTo
        );

        List<TestTask> tasks = partitions
            .stream()
            .map(partition -> new TestTask(partition.startNode(), partition.nodeCount()))
            .collect(Collectors.toList());

        assertTaskRanges(tasks);
    }

    @Test
    void testAlignmentWithTaskSupplier() {
        long alignTo = 64;
        long nodeCount = 200;
        int concurrency = 2;

        var tasks = PartitionUtils.numberAlignedPartitioning(
            concurrency,
            nodeCount,
            alignTo,
            partition -> new TestTask(partition.startNode(), partition.nodeCount())
        );

        assertTaskRanges(tasks);
    }

    private void assertTaskRanges(List<TestTask> tasks) {
        assertEquals(2, tasks.size());
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 0 && t.nodeCount == 128),
            formatWithLocale("Expected task with start %d and nodeCount %d, but found %s", 0, 128, tasks)
        );
        assertTrue(
            tasks.stream().anyMatch((t) -> t.start == 128 && t.nodeCount == 72),
            formatWithLocale("Expected task with start %d and nodeCount %d, but found %s", 128, 72, tasks)
        );
    }

    //@formatter:off
    static Stream<Arguments> ranges() {
        return Stream.of(
            Arguments.of(1, 42, List.of(Partition.of(0, 42))),
            Arguments.of(1, 42_000, List.of(Partition.of(0, 42_000))),
            Arguments.of(4, 40_000, List.of(
                Partition.of(0, DEFAULT_BATCH_SIZE),
                Partition.of(10_000, DEFAULT_BATCH_SIZE),
                Partition.of(20_000, DEFAULT_BATCH_SIZE),
                Partition.of(30_000, DEFAULT_BATCH_SIZE)
            )),
            Arguments.of(4, 42_000, List.of(
                Partition.of(0            , DEFAULT_BATCH_SIZE + 500),
                Partition.of(10_000 +  500, DEFAULT_BATCH_SIZE + 500),
                Partition.of(20_000 + 1000, DEFAULT_BATCH_SIZE + 500),
                Partition.of(30_000 + 1500, DEFAULT_BATCH_SIZE + 500)
            ))
        );
    }
    //@formatter:on

    @ParameterizedTest
    @MethodSource("ranges")
    void testRangePartitioning(int concurrency, long nodeCount, List<Partition> expectedPartitions) {
        assertEquals(
            expectedPartitions,
            PartitionUtils.rangePartition(concurrency, nodeCount, Function.identity(), Optional.empty())
        );
    }

    @Test
    void testDegreePartitioning() {
        var concurrency = 4;

        var graph = RandomGraphGenerator.builder()
            .nodeCount(DEFAULT_BATCH_SIZE)
            .averageDegree(concurrency)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .allocationTracker(AllocationTracker.empty())
            .build()
            .generate();

        var expectedPartitionSize = graph.nodeCount() / concurrency;
        var partitions = PartitionUtils.degreePartition(graph, concurrency, Functions.identity(), Optional.empty());

        assertThat(partitions.size()).isEqualTo(concurrency);
        for (int i = 0; i < concurrency; i++) {
            assertThat(partitions.get(i).nodeCount()).isCloseTo(expectedPartitionSize, within(10L));
        }
    }

    @Test
    void testDegreePartitioningWithBatchSize() {
        Graph graph = fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)"
        );

        var partitions = PartitionUtils.degreePartitionWithBatchSize(graph, 2, Function.identity());
        assertEquals(2, partitions.size());
        assertEquals(0, partitions.get(0).startNode());
        assertEquals(2, partitions.get(0).nodeCount());
        assertEquals(2, partitions.get(1).startNode());
        assertEquals(1, partitions.get(1).nodeCount());
    }

    @Test
    void testDegreePartitioningWithNodeFilter() {
        Graph graph = fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)"
        );
        BitSet nodeFilter = new BitSet(graph.nodeCount());
        nodeFilter.set(0);
        nodeFilter.set(2);

        var partitions = PartitionUtils.degreePartitionWithBatchSize(
            new SetBitsIterable(nodeFilter).primitiveLongIterator(), graph::degree, 2, Function.identity()
        );
        assertEquals(1, partitions.size());
        assertEquals(0, partitions.get(0).startNode());
        assertEquals(3, partitions.get(0).nodeCount());
    }

    static class TestTask implements Runnable {

        public final long start;
        public final long nodeCount;

        TestTask(long start, long nodeCount) {
            this.start = start;
            this.nodeCount = nodeCount;
        }

        @Override
        public void run() {

        }

        @Override
        public String toString() {
            return formatWithLocale("(%d, %d)", start, nodeCount);
        }
    }

}
