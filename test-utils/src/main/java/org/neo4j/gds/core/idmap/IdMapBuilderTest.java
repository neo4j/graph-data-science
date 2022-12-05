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
package org.neo4j.gds.core.idmap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.mem.HugeArrays;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.api.IdMap.NOT_FOUND;

public abstract class IdMapBuilderTest {

    // Runs per property test
    private static final int TRIES = 42;

    protected abstract IdMapBuilder builder(long capacity, int concurrency);

    @Provide
    Arbitrary<Integer> concurrencies() {
        return Arbitraries.of(1, 2, 4, 8);
    }

    @Provide
    Arbitrary<Integer> nodeCounts() {
        return Arbitraries.of(1, 100, 1000, HugeArrays.PAGE_SIZE, 2 * HugeArrays.PAGE_SIZE, 10 * HugeArrays.PAGE_SIZE);
    }

    @Provide
    Arbitrary<Integer> idOffsets() {
        return Arbitraries.of(0, HugeArrays.PAGE_SIZE, 10 * HugeArrays.PAGE_SIZE, 100 * HugeArrays.PAGE_SIZE);
    }

    @Test
    void testAllocatedSize() {
        long capacity = 4096;
        int allocation = 1337;

        var builder = builder(capacity, 1);
        var idMapAllocator = builder.allocate(allocation);

        assertThat(idMapAllocator.allocatedSize()).as("allocated size").isEqualTo(allocation);
    }

    @Test
    void testSingleElement() {
        var idMap = buildIdMapFrom(new long[]{42L}, 1);

        assertThat(idMap.nodeCount()).isEqualTo(1);
        assertThat(idMap.toMappedNodeId(42)).isEqualTo(0);
        assertThat(idMap.toOriginalNodeId(0)).isEqualTo(42);
        assertThat(idMap.highestOriginalId()).isEqualTo(42);
    }

    @Property(tries = TRIES)
    void testNodeCount(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMap = buildIdMapFrom(originalIds, concurrency);
        assertThat(idMap.nodeCount()).as("node count").isEqualTo(originalIds.length);
    }

    @Property(tries = TRIES)
    void testContains(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMapAndHighestId = buildFrom(originalIds, concurrency);
        var idMap = idMapAndHighestId.idMap();
        var highestOriginalId = idMapAndHighestId.highestOriginalId();

        assertThat(idMap.nodeCount()).as("node count").isEqualTo(originalIds.length);
        Arrays.stream(originalIds).forEach(originalId -> assertThat(idMap.contains(originalId))
            .as(originalId + " is contained in IdMap")
            .isTrue());

        // Create some random ids that aren't originalIds and
        // check that they are not contained in the map.
        Arrays.sort(originalIds);
        var rng = new SplittableRandom();
        LongStream
            .range(0, originalIds.length)
            .map(__ -> rng.nextLong(highestOriginalId + 1))
            .filter(id -> Arrays.binarySearch(originalIds, id) < 0)
            .forEach(id -> assertThat(idMap.contains(id)).isFalse());
    }

    @Property(tries = TRIES)
    void testHighestOriginalId(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMapAndHighestId = buildFrom(originalIds, concurrency);

        assertThat(idMapAndHighestId.idMap().highestOriginalId()).isEqualTo(idMapAndHighestId.highestOriginalId());
    }

    @Property(tries = TRIES)
    void testToMappedNodeId(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMap = buildIdMapFrom(originalIds, concurrency);
        var mappedNodeIds = new long[originalIds.length];
        var actualNodeCount = idMap.nodeCount();

        Arrays.fill(mappedNodeIds, NOT_FOUND);

        for (int i = 0; i < originalIds.length; i++) {
            var mappedNodeId = idMap.toMappedNodeId(originalIds[i]);
            assertThat(mappedNodeId).isNotEqualTo(NOT_FOUND);
            mappedNodeIds[i] = mappedNodeId;
        }

        Arrays.sort(mappedNodeIds);
        assertThat(mappedNodeIds).doesNotHaveDuplicates();
        assertThat(mappedNodeIds[mappedNodeIds.length - 1]).isEqualTo(actualNodeCount - 1);
    }

    @Property(tries = TRIES)
    void testToRootNodeId(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMap = buildIdMapFrom(originalIds, concurrency);
        var rootNodeIds = new long[originalIds.length];
        var actualNodeCount = idMap.nodeCount();

        Arrays.fill(rootNodeIds, NOT_FOUND);

        for (int mappedId = 0; mappedId < originalIds.length; mappedId++) {
            var rootNodeId = idMap.toRootNodeId(mappedId);
            assertThat(rootNodeId).isNotEqualTo(NOT_FOUND);
            rootNodeIds[mappedId] = rootNodeId;
        }

        Arrays.sort(rootNodeIds);
        assertThat(rootNodeIds).doesNotHaveDuplicates();
        assertThat(rootNodeIds[rootNodeIds.length - 1]).isEqualTo(actualNodeCount - 1);
    }

    @Property(tries = TRIES)
    void testToOriginalNodeId(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var idMap = buildIdMapFrom(originalIds, concurrency);
        var actualOriginalIds = new long[originalIds.length];
        Arrays.fill(actualOriginalIds, -1);

        for (int mappedId = 0; mappedId < idMap.nodeCount(); mappedId++) {
            actualOriginalIds[mappedId] = idMap.toOriginalNodeId(mappedId);
        }

        Arrays.sort(originalIds);
        Arrays.sort(actualOriginalIds);
        assertThat(actualOriginalIds).isEqualTo(originalIds);
    }

    @Property(tries = TRIES)
    void testBuildParallel(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var bufferSize = 1000;
        var highestOriginalId = max(originalIds);
        var idMapBuilder = builderFromHighestOriginalId(highestOriginalId, concurrency);

        var tasks = PartitionUtils.rangePartition(concurrency, nodeCount, partition -> (Runnable) () -> {
            long nodesProcessed = 0;
            long[] buffer = new long[bufferSize];
            int offset = (int) partition.startNode();
            while (nodesProcessed < partition.nodeCount()) {
                int batchLength = Math.min(bufferSize, (int) (partition.nodeCount() - nodesProcessed));
                System.arraycopy(originalIds, offset, buffer, 0, batchLength);
                var allocator = idMapBuilder.allocate(batchLength);
                allocator.insert(Arrays.copyOfRange(buffer, 0, batchLength));
                nodesProcessed += batchLength;
                offset += batchLength;
            }
        }, Optional.empty());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var idMap = idMapBuilder.build(LabelInformationBuilders.allNodes(), highestOriginalId, concurrency);

        assertThat(idMap.nodeCount()).as("node count").isEqualTo(nodeCount);
        assertThat(idMap.highestOriginalId()).as("highest original id").isEqualTo(highestOriginalId);

        for (long originalId : originalIds) {
            assertThat(idMap.contains(originalId)).as("contains original id " + originalId).isTrue();
        }
    }

    @Property(tries = TRIES)
    void testLabels(
        @ForAll("nodeCounts") int nodeCount,
        @ForAll("idOffsets") int idOffset,
        @ForAll("concurrencies") int concurrency,
        @ForAll long seed
    ) {
        var originalIds = generateShuffledIds(idOffset, nodeCount, seed);
        var allLabels = new NodeLabel[]{
            NodeLabel.of("R"),
            NodeLabel.of("U"),
            NodeLabel.of("S"),
            NodeLabel.of("T"),
        };
        var rng = new Random(seed);
        var expectedLabels = new HashMap<Long, List<NodeLabel>>();
        var noLabel = List.of(NodeLabel.ALL_NODES);

        var idMap = buildFromWithLabels(originalIds, concurrency, originalId -> {
            var labelCount = rng.nextInt(allLabels.length);
            var labels = labelCount > 0
                ? Arrays.stream(allLabels).limit(labelCount).collect(Collectors.toList())
                : noLabel;
            expectedLabels.put(originalId, labels);
            return labels;
        }).idMap();

        for (int mappedNodeId = 0; mappedNodeId < idMap.nodeCount(); mappedNodeId++) {
            var originalNodeId = idMap.toOriginalNodeId(mappedNodeId);
            assertThat(idMap.nodeLabels(mappedNodeId)).containsAll(expectedLabels.get(originalNodeId));
        }
    }

    private IdMapBuilder builderFromHighestOriginalId(long highestOriginalId, int concurrency) {
        return builder(highestOriginalId + 1, concurrency);
    }

    @ValueClass
    public interface IdMapAndHighestId {
        IdMap idMap();

        long highestOriginalId();
    }

    private IdMap buildIdMapFrom(long[] originalIds, int concurrency) {
        return buildFrom(originalIds, concurrency).idMap();
    }

    private IdMapAndHighestId buildFrom(long[] originalIds, int concurrency) {
        return buildFromWithLabels(originalIds, concurrency, Optional.empty());
    }

    private IdMapAndHighestId buildFromWithLabels(
        long[] originalIds,
        int concurrency,
        LongFunction<List<NodeLabel>> labelFn
    ) {
        return buildFromWithLabels(originalIds, concurrency, Optional.ofNullable(labelFn));
    }

    private IdMapAndHighestId buildFromWithLabels(
        long[] originalIds,
        int concurrency,
        Optional<LongFunction<List<NodeLabel>>> labelFn
    ) {
        // number of ids we want to insert at once
        int batchLength = originalIds.length;
        // the highest original id defines the size of the specific IdMap data structures
        long highestOriginalId = max(originalIds);

        var builder = builderFromHighestOriginalId(highestOriginalId, 1);
        var idMapAllocator = builder.allocate(batchLength);

        idMapAllocator.insert(originalIds);

        var labelInformationBuilder = labelFn.map(lFn -> {
            var multiLabelBuilder = LabelInformationBuilders.multiLabelWithCapacity(originalIds.length);
            for (long originalNodeId : originalIds) {
                lFn.apply(originalNodeId).forEach(label -> multiLabelBuilder.addNodeIdToLabel(label, originalNodeId));
            }
            return multiLabelBuilder;
        }).orElseGet(LabelInformationBuilders::allNodes);

        return ImmutableIdMapAndHighestId.builder()
            .idMap(builder.build(labelInformationBuilder, highestOriginalId, concurrency))
            .highestOriginalId(highestOriginalId)
            .build();
    }

    private static long[] generateShuffledIds(int offset, int size, long seed) {
        return shuffle(generate(offset, size, seed), seed);
    }

    private static long[] generate(int offset, int size, long seed) {
        var rng = new Random(seed);
        var ids = new long[size];

        long nextId = offset;
        int count = 0;

        while (count < size) {
            if (rng.nextBoolean()) {
                ids[count++] = nextId;
            }
            nextId++;
        }

        return ids;
    }

    // Fisher and Yates
    private static long[] shuffle(long[] array, long seed) {
        var rng = new SplittableRandom(seed);
        var len = array.length;
        for (int i = 0; i < len - 2; i++) {
            int j = rng.nextInt(i, len);
            long tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        }
        return array;
    }

    private static long max(long[] ids) {
        return Arrays.stream(ids).max().orElseThrow();
    }
}
