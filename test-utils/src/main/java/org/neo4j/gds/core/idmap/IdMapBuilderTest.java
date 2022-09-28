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
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.IdMapBuilder;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.utils.paged.HugeArrays;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

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

public abstract class IdMapBuilderTest {

    // The highest original id that is support in _all_ IdMap implementations
    public static final long HIGHEST_SUPPORTED_ORIGINAL_ID = (1L << 36) - 1;

    protected abstract IdMapBuilder builder(long capacity, int concurrency);

    @Provide
    Arbitrary<Integer> concurrencies() {
        return Arbitraries.of(1, 2, 4, 8);
    }

    @Provide
    Arbitrary<long[]> sparseIds() {
        return Arbitraries
            .longs()
            .between(0, 10 * HugeArrays.PAGE_SIZE)
            .array(long[].class)
            .ofMinSize(1)
            .ofMaxSize(HugeArrays.PAGE_SIZE)
            .uniqueElements();
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
        var idMap = buildFromOriginalIds(new long[]{42L}, 1).idMap();

        assertThat(idMap.nodeCount()).isEqualTo(1);
        assertThat(idMap.toMappedNodeId(42)).isEqualTo(0);
        assertThat(idMap.toOriginalNodeId(0)).isEqualTo(42);
        assertThat(idMap.highestOriginalId()).isEqualTo(42);
    }

    @Property(tries = 5)
    void testNodeCount(@ForAll("sparseIds") long[] originalIds) {
        var idMap = buildFromOriginalIds(originalIds, 1).idMap();
        assertThat(idMap.nodeCount()).isEqualTo(originalIds.length);
    }

    @Property(tries = 5)
    void testContains(@ForAll("sparseIds") long[] originalIds) {
        var idMapAndHighestId = buildFromOriginalIds(originalIds, 1);
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

    @Property(tries = 5)
    void testHighestOriginalId(@ForAll("sparseIds") long[] originalIds) {
        var idMapAndHighestId = buildFromOriginalIds(originalIds, 1);

        assertThat(idMapAndHighestId.idMap().highestOriginalId()).isEqualTo(idMapAndHighestId.highestOriginalId());
    }

    @Property(tries = 5)
    void testToMappedNodeId(@ForAll("sparseIds") long[] originalIds) {
        var idMap = buildFromOriginalIds(originalIds, 1).idMap();
        var mappedIds = new long[originalIds.length];
        var nodeCount = idMap.nodeCount();
        Arrays.fill(mappedIds, -1);

        for (int i = 0; i < originalIds.length; i++) {
            mappedIds[i] = idMap.toMappedNodeId(originalIds[i]);
        }

        Arrays.sort(mappedIds);

        assertThat(mappedIds[mappedIds.length - 1]).isEqualTo(nodeCount - 1);

        for (long mappedId : mappedIds) {
            assertThat(mappedId).isGreaterThan(-1);
        }
    }

    @Property(tries = 5)
    void testToRootNodeId(@ForAll("sparseIds") long[] originalIds) {
        var idMap = buildFromOriginalIds(originalIds, 1).idMap();
        var rootNodeIds = new long[originalIds.length];
        var nodeCount = idMap.nodeCount();

        Arrays.fill(rootNodeIds, -1);

        for (int mappedId = 0; mappedId < originalIds.length; mappedId++) {
            rootNodeIds[mappedId] = idMap.toRootNodeId(mappedId);
        }

        Arrays.sort(rootNodeIds);

        assertThat(rootNodeIds[rootNodeIds.length - 1]).isEqualTo(nodeCount - 1);

        for (long rootNodeId : rootNodeIds) {
            assertThat(rootNodeId).isGreaterThan(-1);
        }
    }

    @Property(tries = 5)
    void testToOriginalNodeId(@ForAll("sparseIds") long[] expectedOriginalIds) {
        var idMap = buildFromOriginalIds(expectedOriginalIds, 1).idMap();
        var actualOriginalIds = new long[expectedOriginalIds.length];
        Arrays.fill(actualOriginalIds, -1);

        for (int mappedId = 0; mappedId < idMap.nodeCount(); mappedId++) {
            actualOriginalIds[mappedId] = idMap.toOriginalNodeId(mappedId);
        }

        Arrays.sort(expectedOriginalIds);
        Arrays.sort(actualOriginalIds);
        assertThat(actualOriginalIds).isEqualTo(expectedOriginalIds);
    }

    @Property(tries = 5)
    void testBuildParallel(
        @ForAll long seed,
        @ForAll @IntRange(min = HugeArrays.PAGE_SIZE, max = 10 * HugeArrays.PAGE_SIZE) int nodeCount,
        @ForAll("concurrencies") int concurrency
    ) {
        var originalIds = generateOriginalIds(nodeCount, seed);
        var bufferSize = 1000;
        var highestOriginalId = highestOriginalId(originalIds);
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

        var idMap = idMapBuilder.build(LabelInformation.single(NodeLabel.ALL_NODES), highestOriginalId, concurrency);

        assertThat(idMap.nodeCount()).as("node count").isEqualTo(nodeCount);
        assertThat(idMap.highestOriginalId()).as("highest original id").isEqualTo(highestOriginalId);

        for (long originalId : originalIds) {
            assertThat(idMap.contains(originalId)).as("contains original id " + originalId).isTrue();
        }
    }

    @Property(tries = 5)
    void testLabels(@ForAll("sparseIds") long[] originalIds) {
        var allLabels = new NodeLabel[]{NodeLabel.of("A"), NodeLabel.of("B"), NodeLabel.of("C")};
        var rng = new Random();
        var expectedLabels = new HashMap<Long, List<NodeLabel>>();

        var idMap = buildFromOriginalIds(originalIds, 1, Optional.of(originalId -> {
            var labelCount = rng.nextInt(1, allLabels.length);
            var labels = Arrays.stream(allLabels).limit(labelCount).collect(Collectors.toList());
            expectedLabels.put(originalId, labels);
            return labels;
        })).idMap();

        for (int mappedNodeId = 0; mappedNodeId < idMap.nodeCount(); mappedNodeId++) {
            var originalNodeId = idMap.toOriginalNodeId(mappedNodeId);
            assertThat(idMap.nodeLabels(mappedNodeId)).isEqualTo(expectedLabels.get(originalNodeId));
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

    private IdMapAndHighestId buildFromOriginalIds(long[] originalIds, int concurrency) {
        return buildFromOriginalIds(originalIds, concurrency, Optional.empty());
    }

    private IdMapAndHighestId buildFromOriginalIds(
        long[] originalIds,
        int concurrency,
        Optional<LongFunction<List<NodeLabel>>> labelFn
    ) {
        // number of ids we want to insert at once
        int batchLength = originalIds.length;
        // the highest original id defines the size of the specific IdMap data structures
        long highestOriginalId = Arrays.stream(originalIds).max().getAsLong();

        var builder = builderFromHighestOriginalId(highestOriginalId, 1);
        var idMapAllocator = builder.allocate(batchLength);

        idMapAllocator.insert(originalIds);

        var labelInformationBuilder = labelFn.map(lFn -> {
            var multiLabelBuilder = LabelInformation.builder(originalIds.length);
            for (long originalNodeId : originalIds) {
                lFn.apply(originalNodeId).forEach(label -> multiLabelBuilder.addNodeIdToLabel(label, originalNodeId));
            }
            return multiLabelBuilder;
        }).orElseGet(() -> LabelInformation.single(NodeLabel.ALL_NODES));

        return ImmutableIdMapAndHighestId.builder()
            .idMap(builder.build(labelInformationBuilder, highestOriginalId, concurrency))
            .highestOriginalId(highestOriginalId)
            .build();
    }

    static long[] generateOriginalIds(int size, long seed) {
        var rng = new Random(seed);
        var ids = new long[size];

        long nextId = 0;
        int count = 0;

        while (count < size) {
            if (rng.nextBoolean()) {
                ids[count++] = nextId;
            }
            nextId++;
        }

        return ids;
    }

    static long highestOriginalId(long[] ids) {
        return Arrays.stream(ids).max().orElse(-1);
    }
}
