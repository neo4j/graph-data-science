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
package org.neo4j.gds.core.compression.mixed;

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.ImmutableMemoryInfo;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListBuilderFactory;
import org.neo4j.gds.api.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.api.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.MemoryTracker;
import org.neo4j.gds.core.compression.packed.Address;
import org.neo4j.gds.core.compression.packed.AdjacencyPacking;
import org.neo4j.gds.core.compression.packed.PackedCompressor;
import org.neo4j.gds.core.compression.varlong.DeltaVarLongCompressor;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.ArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public final class MixedCompressor implements AdjacencyCompressor {

    /**
     * The threshold for when to use packing. Nodes, that have a degree lower than the threshold,
     * will use var-long compression. We picked 8 blocks, i.e. a degree of 512, based on intense
     * research ... and by that we mean we tried a few different values and this one seemed to work.
     */
    private static final int PACKING_DEGREE_THRESHOLD = AdjacencyPacking.BLOCK_SIZE * 8;

    private final AdjacencyCompressor packedCompressor;
    private final AdjacencyCompressor vLongCompressor;

    private MixedCompressor(
        AdjacencyCompressor packedCompressor,
        AdjacencyCompressor vLongCompressor
    ) {
        this.packedCompressor = packedCompressor;
        this.vLongCompressor = vLongCompressor;
    }

    static boolean usePacking(int degree) {
        return degree > PACKING_DEGREE_THRESHOLD;
    }

    @Override
    public int compress(long nodeId, long[] targets, long[][] properties, int degree) {
        if (usePacking(degree)) {
            return this.packedCompressor.compress(nodeId, targets, properties, degree);
        } else {
            return this.vLongCompressor.compress(nodeId, targets, properties, degree);
        }
    }

    @Override
    public void close() {
        this.packedCompressor.close();
        this.vLongCompressor.close();
    }

    public static AdjacencyCompressorFactory factory(
        LongSupplier nodeCountSupplier,
        AdjacencyListBuilderFactory<Address, ? extends AdjacencyList, long[], ? extends AdjacencyProperties> packedAdjacencyListBuilder,
        AdjacencyListBuilderFactory<byte[], ? extends AdjacencyList, long[], ? extends AdjacencyProperties> vlongAdjacencyListBuilder,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation,
        MemoryTracker memoryTracker
    ) {
        var relationshipCounter = new LongAdder();

        var packedCompressorFactory = PackedCompressor.factory(
            nodeCountSupplier,
            packedAdjacencyListBuilder,
            propertyMappings,
            aggregations,
            noAggregation,
            memoryTracker
        );

        var vlongCompressorFactory = DeltaVarLongCompressor.factory(
            nodeCountSupplier,
            vlongAdjacencyListBuilder,
            propertyMappings,
            aggregations,
            noAggregation,
            memoryTracker
        );

        return new Factory(nodeCountSupplier, relationshipCounter, packedCompressorFactory, vlongCompressorFactory);
    }

    static class Factory implements AdjacencyCompressorFactory {

        private final LongSupplier nodeCountSupplier;
        private final LongAdder relationshipCounter;
        private final AdjacencyCompressorFactory packedCompressorFactory;
        private final AdjacencyCompressorFactory vlongCompressorFactory;


        Factory(
            LongSupplier nodeCountSupplier,
            LongAdder relationshipCounter,
            AdjacencyCompressorFactory packedCompressorFactory,
            AdjacencyCompressorFactory vlongCompressorFactory
        ) {
            this.nodeCountSupplier = nodeCountSupplier;
            this.relationshipCounter = relationshipCounter;
            this.packedCompressorFactory = packedCompressorFactory;
            this.vlongCompressorFactory = vlongCompressorFactory;
        }

        @Override
        public void init() {
            var nodeCount = this.nodeCountSupplier.getAsLong();
            HugeIntArray adjacencyDegrees = HugeIntArray.newArray(nodeCount);
            HugeLongArray adjacencyOffsets = HugeLongArray.newArray(nodeCount);
            HugeLongArray propertyOffsets = HugeLongArray.newArray(nodeCount);

            init(adjacencyDegrees, adjacencyOffsets, propertyOffsets);
        }

        @Override
        public void init(HugeIntArray degrees, HugeLongArray adjacencyOffsets, HugeLongArray propertyOffsets) {
            this.packedCompressorFactory.init(degrees, adjacencyOffsets, propertyOffsets);
            this.vlongCompressorFactory.init(degrees, adjacencyOffsets, propertyOffsets);
        }

        @Override
        public AdjacencyCompressor createCompressor() {
            var packedCompressor = packedCompressorFactory.createCompressor();
            var vlongCompressor = vlongCompressorFactory.createCompressor();
            return new MixedCompressor(packedCompressor, vlongCompressor);
        }

        @Override
        public LongAdder relationshipCounter() {
            return relationshipCounter;
        }

        @Override
        public AdjacencyListsWithProperties build(boolean allowReordering) {
            var packedAdjacencyList = this.packedCompressorFactory.build(false);
            var vlongAdjacencyList = this.vlongCompressorFactory.build(false);

            var memoryInfo = mergeMemoryInfo(
                packedAdjacencyList.adjacency().memoryInfo(),
                vlongAdjacencyList.adjacency().memoryInfo()
            );

            var mixedAdjacencyList = new MixedAdjacencyList(
                packedAdjacencyList.adjacency(),
                vlongAdjacencyList.adjacency(),
                memoryInfo
            );

            var mixedAdjacencyProperties = new ArrayList<AdjacencyProperties>(packedAdjacencyList.properties().size());

            for (int i = 0; i < packedAdjacencyList.properties().size(); i++) {
                var left = packedAdjacencyList.properties().get(i);
                var right = vlongAdjacencyList.properties().get(i);
                var mixedProperties = new MixedAdjacencyProperties(vlongAdjacencyList.adjacency(), left, right);
                mixedAdjacencyProperties.add(mixedProperties);
            }

            return ImmutableAdjacencyListsWithProperties
                .builder()
                .adjacency(mixedAdjacencyList)
                .addAllProperties(mixedAdjacencyProperties)
                .relationshipCount(relationshipCounter.longValue())
                .build();
        }

        private static AdjacencyList.MemoryInfo mergeMemoryInfo(
            AdjacencyList.MemoryInfo packed,
            AdjacencyList.MemoryInfo vlong
        ) {
            return ImmutableMemoryInfo.builder()
                .pages(packed.pages() + vlong.pages())
                .pageSizes(vlong.pageSizes().merge(packed.pageSizes()))
                // We use the vlong on heap data structures (pages, offsets and degrees),
                // where offsets and degrees are shared with the packed adjacency list.
                .bytesOnHeap(vlong.bytesOnHeap())
                .heapAllocations(vlong.heapAllocations())
                // We only need to track the off heap data structures of the packed adjacency list.
                .bytesOffHeap(packed.bytesOffHeap())
                .nativeAllocations(packed.nativeAllocations())
                // We only need to track the header statistics of the packed adjacency list.
                .headerBits(packed.headerBits())
                .headerAllocations(packed.headerAllocations())
                .blockStatistics(packed.blockStatistics())
                .build();
        }
    }

}
