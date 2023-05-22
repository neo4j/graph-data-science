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
package org.neo4j.gds.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.eclipse.collections.impl.block.factory.Functions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.partition.IteratorPartition;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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

    @Test
    void testAlignmentWithMaxSize() {
        long alignTo = 42;
        long maxSize = 100;
        long nodeCount = 400;
        int concurrency = 3;

        List<Partition> partitions = PartitionUtils.numberAlignedPartitioningWithMaxSize(
            concurrency,
            nodeCount,
            alignTo,
            maxSize
        );

        List<TestTask> tasks = partitions
            .stream()
            .map(partition -> new TestTask(partition.startNode(), partition.nodeCount()))
            .collect(Collectors.toList());

        assertTaskRangesWithMaxSize(tasks);
    }

    @Test
    void testAlignmentWithMaxSizeWithTaskSupplier() {
        long alignTo = 42;
        long maxSize = 100;
        long nodeCount = 400;
        int concurrency = 3;

        var tasks = PartitionUtils.numberAlignedPartitioningWithMaxSize(
            concurrency,
            nodeCount,
            alignTo,
            maxSize,
            partition -> new TestTask(partition.startNode(), partition.nodeCount())
        );

        assertTaskRangesWithMaxSize(tasks);
    }

    private void assertTaskRangesWithMaxSize(List<TestTask> tasks) {
        assertThat(tasks)
            .hasSize(5)
            .usingElementComparator(Comparator
                .<TestTask>comparingLong(task -> task.start)
                .thenComparingLong(task -> task.nodeCount))
            .containsExactlyInAnyOrder(
                new TestTask(0, 84),
                new TestTask(84, 84),
                new TestTask(168, 84),
                new TestTask(252, 84),
                new TestTask(336, 64)
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
                Partition.of(            0, DEFAULT_BATCH_SIZE + 500),
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

    static Stream<Arguments> degreeDistributionWithPartitions() {
        return Stream.of(
            Arguments.of(
                "UNIFORM",
                LongStream.of(0, 2500, 5000, 7500)
                    .mapToObj(start -> DegreePartition.of(start, 2500, 10000))
                    .collect(Collectors.toList())
            ),
            Arguments.of("RANDOM", List.of(
                DegreePartition.of(0, 2491, 10046),
                DegreePartition.of(2491, 2526, 10049),
                DegreePartition.of(5017, 2505, 10044),
                DegreePartition.of(7522, 2478, 10054)
            )),
            Arguments.of("POWER_LAW", List.of(
                    DegreePartition.of(0, 3, 9172),
                    DegreePartition.of(3, 6, 9382),
                    DegreePartition.of(9, 53, 10022),
                    DegreePartition.of(62, 9938, 11602)
                )
            )
        );
    }



    @MethodSource("degreeDistributionWithPartitions")
    @ParameterizedTest
    void testDegreePartitioning(String distribution, List<DegreePartition> expectedPartitions) {
        var concurrency = 4;

        var graph = RandomGraphGenerator.builder()
            .nodeCount(10_000)
            .averageDegree(concurrency)
            .seed(42)
            .relationshipDistribution(RelationshipDistribution.parse(distribution))
            .build()
            .generate();

        var partitions = PartitionUtils.degreePartition(graph, concurrency, Functions.identity(), Optional.empty());

        assertThat(partitions.stream().mapToLong(DegreePartition::totalDegree).sum()).isEqualTo(graph.relationshipCount());
        assertThat(partitions).containsExactlyElementsOf(expectedPartitions);

    }

    @Test
    void testDegreePartitionWithMiddleHighDegreeNodes() {
        var nodeCount = 5;
        var degrees = new int[] { 1, 1, 10, 3, 1 };
        var degreesPerPartition = 3;

        List<DegreePartition> partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeCount,
            idx -> degrees[(int) idx],
            degreesPerPartition,
            Function.identity()
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 2, 2),
            DegreePartition.of(2, 1, 10),
            DegreePartition.of(3, 1, 3),
            DegreePartition.of(4, 1, 1)
        );
    }

    @Test
    void testDegreePartitionWithAlternatingHighDegreeNodes() {
        var nodeCount = 5;
        var degrees = new int[] { 1, 10, 1, 10, 1 };
        var degreesPerPartition = 3;

        List<DegreePartition> partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeCount,
            idx -> degrees[(int) idx],
            degreesPerPartition,
            Function.identity()
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 2, 11),
            DegreePartition.of(2, 2, 11),
            DegreePartition.of(4, 1, 1)
        );
    }

    @Test
    void testDegreePartitionWithPotentiallySmallLast() {
        var nodeCount = 4;
        var degrees = new int[] { 30, 30, 30, 1 };
        var degreesPerPartition = 30;

        List<DegreePartition> partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeCount,
            idx -> degrees[(int) idx],
            degreesPerPartition,
            Function.identity()
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 1, 30),
            DegreePartition.of(1, 1, 30),
            DegreePartition.of(2, 2, 31)
        );
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

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 1, 2),
            DegreePartition.of(1, 2, 2)
        );
    }

    @Test
    void testCustomDegreePartitioningWithBatchSize() {
        Graph graph = fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)" +
            "(a)-->(d)" +
            "(a)-->(e)"
        );


        int[] weights = {1, 6, 6, 1, 1};
        LongToIntFunction weightFunction = x -> weights[(int) x];
        var partitions = PartitionUtils.customDegreePartitionWithBatchSize(
            graph,
            3,
            weightFunction,
            Function.identity(),
            Optional.of(1),
            Optional.of(9L)
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 2, 7),
            DegreePartition.of(2, 1, 6),
            DegreePartition.of(3, 2, 2)
        );
    }

    @Test
    void testCustomDegreePartitioningWithBatchSizeWithoutTotalSumGiven() {
        Graph graph = fromGdl(
            "(a)-->(b)" +
            "(a)-->(c)" +
            "(b)-->(a)" +
            "(b)-->(c)" +
            "(a)-->(d)" +
            "(a)-->(e)"
        );


        int[] weights = {1, 6, 6, 1, 1};
        LongToIntFunction weightFunction = x -> weights[(int) x];
        var partitions = PartitionUtils.customDegreePartitionWithBatchSize(
            graph,
            3,
            weightFunction,
            Function.identity(),
            Optional.of(1),
            Optional.empty()
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 2, 7),
            DegreePartition.of(2, 1, 6),
            DegreePartition.of(3, 2, 2)
        );
    }

    @Test
    void testDegreePartitioningOnBitSet() {
        int[] degrees = {2, 1, 2, 4};

        BitSet nodeFilter = new BitSet(degrees.length);
        nodeFilter.set(0);
        nodeFilter.set(1);
        nodeFilter.set(3);

        var partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeFilter, i -> degrees[(int) i], 2, Function.identity()
        );

        assertThat(partitions.stream().map(IteratorPartition::materialize)).containsExactlyInAnyOrder(
            new long[] {0},
            new long[] {1, 3}
        );
    }

    @Test
    void testDegreePartitioningOnBitSetExceedsLimit() {
        int[] degrees = {4, 1, 4, 0};

        BitSet nodeFilter = new BitSet(degrees.length);
        nodeFilter.set(0);
        nodeFilter.set(1);
        nodeFilter.set(2);

        int degreesPerBatch = 2;
        var partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeFilter, i -> degrees[(int) i], degreesPerBatch, Function.identity()
        );

        assertThat(partitions.stream().map(IteratorPartition::materialize)).containsExactlyInAnyOrder(
            new long[] {0},
            new long[] {1, 2}
        );
    }

    @Test
    void testDegreePartitioningWithForcedNumberOfPartitionsShortcoming() {
        // There is a drawback with forcing the number of partitions. This returns a not optimal partitioning,
        // but it's required because each thread has exactly one partition.
        // See next test for the optimal partitioning.

        var nodeCount = 6;
        int[] degrees = {20, 20, 20, 20, 10, 10};
        var concurrency = 4;
        var degreesPerPartition = ParallelUtil.adjustedBatchSize(Arrays.stream(degrees).sum(), concurrency, 1);

        List<DegreePartition> partitions = PartitionUtils.degreePartitionWithBatchSize(
            nodeCount,
            idx -> degrees[(int) idx],
            degreesPerPartition,
            Function.identity()
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 1, 20),
            DegreePartition.of(1, 1, 20),
            DegreePartition.of(2, 1, 20),
            DegreePartition.of(3, 1, 20),
            DegreePartition.of(4, 2, 20)
        );
    }

    @Test
    void testDegreePartitioningStreamWithMorePartitionsThanThreads() {

        // 4 threads
        // 4 partitions (not good):
        // {20} {20} {20} {20, 10, 10}
        // 5 partitions (not good):
        // {20} {20} {20} {20} {10, 10}
        // 6 partitions (good):
        // {20} {20} {20} {20} {10} {10}

        int[] degrees = {20, 20, 20, 20, 10, 10};
        var nodeCount = degrees.length;
        long relCount = Arrays.stream(degrees).sum();
        var concurrency = 4;

        Stream<DegreePartition> partitions = PartitionUtils.degreePartitionStream(
            nodeCount,
            relCount,
            concurrency,
            idx -> degrees[(int) idx]
        );

        assertThat(partitions).containsExactly(
            DegreePartition.of(0, 1, 20),
            DegreePartition.of(1, 1, 20),
            DegreePartition.of(2, 1, 20),
            DegreePartition.of(3, 1, 20),
            DegreePartition.of(4, 1, 10),
            DegreePartition.of(5, 1, 10)
        );
    }

    @Test
    void testBlockAlignedPartitioning() {
        var blockShift = 3; // 2^3 = 8 ids per block

        var sortedArray = HugeLongArray.of(
            /* block 0 */ 0, 3, 5,
            /* block 1 */ 9, 10, 11, 12, 13, 14, 15,
            /* block 2 */
            /* block 3 */ 24, 28, 30, 31,
            /* block 4 */ 32
        );

        var partitionIterator = PartitionUtils.blockAlignedPartitioning(
            sortedArray,
            blockShift,
            partition -> partition
        );

        // 😿 Java
        var partitions = StreamSupport
            .stream(((Iterable<Partition>) () -> partitionIterator).spliterator(), false)
            .collect(Collectors.toList());

        assertThat(partitions)
            .containsExactly(
                Partition.of(0, 3),
                Partition.of(3, 7),
                Partition.of(10, 4),
                Partition.of(14, 1)
            );
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
