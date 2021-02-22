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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressor;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorFactory;
import org.neo4j.graphalgo.core.compress.AdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.LongArrayBuffer;
import org.neo4j.graphalgo.core.compress.CompressedProperties;
import org.neo4j.graphalgo.core.compress.CompressedTopology;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;

public final class DeltaVarLongCompressor implements AdjacencyCompressor {

    public static final class Factory implements AdjacencyCompressorFactory {

        private final AdjacencyOffsetsFactory adjacencyOffsetsFactory;

        public Factory(AdjacencyOffsetsFactory adjacencyOffsetsFactory) {
            this.adjacencyOffsetsFactory = adjacencyOffsetsFactory;
        }

        @Override
        public AdjacencyCompressor create(
            long nodeCount,
            AdjacencyListBuilder adjacencyBuilder,
            AdjacencyListBuilder[] propertyBuilders,
            Aggregation[] aggregations,
            boolean noAggregation,
            AllocationTracker tracker
        ) {
            return new DeltaVarLongCompressor(
                adjacencyBuilder,
                propertyBuilders,
                adjacencyOffsetsFactory,
                HugeLongArray.newArray(nodeCount, tracker),
                Stream
                    .generate(() -> HugeLongArray.newArray(nodeCount, tracker))
                    .limit(propertyBuilders.length)
                    .toArray(HugeLongArray[]::new),
                noAggregation,
                aggregations
            );
        }
    }

    private final AdjacencyListBuilder adjacencyBuilder;
    private final AdjacencyListBuilder[] propertyBuilders;
    private final AdjacencyListAllocator adjacencyAllocator;
    private final AdjacencyListAllocator[] propertiesAllocators;
    private final AdjacencyOffsetsFactory adjacencyOffsetsFactory;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray[] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    private DeltaVarLongCompressor(
        AdjacencyListBuilder adjacencyBuilder,
        AdjacencyListBuilder[] propertyBuilders,
        AdjacencyOffsetsFactory adjacencyOffsetsFactory,
        HugeLongArray adjacencyOffsets,
        HugeLongArray[] propertyOffsets,
        boolean noAggregation,
        Aggregation[] aggregations
    ) {
        this.adjacencyBuilder = adjacencyBuilder;
        this.propertyBuilders = propertyBuilders;
        this.adjacencyAllocator = null;
        this.propertiesAllocators = null;
        this.adjacencyOffsetsFactory = adjacencyOffsetsFactory;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.noAggregation = noAggregation;
        this.aggregations = aggregations;
    }

    private DeltaVarLongCompressor(
        AdjacencyListBuilder adjacencyBuilder,
        AdjacencyListBuilder[] propertyBuilders,
        AdjacencyListAllocator adjacencyAllocator,
        AdjacencyListAllocator[] propertiesAllocators,
        AdjacencyOffsetsFactory adjacencyOffsetsFactory,
        HugeLongArray adjacencyOffsets,
        HugeLongArray[] propertyOffsets,
        boolean noAggregation,
        Aggregation[] aggregations
    ) {
        this.adjacencyBuilder = adjacencyBuilder;
        this.propertyBuilders = propertyBuilders;
        this.adjacencyAllocator = adjacencyAllocator;
        this.propertiesAllocators = propertiesAllocators;
        this.adjacencyOffsetsFactory = adjacencyOffsetsFactory;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.noAggregation = noAggregation;
        this.aggregations = aggregations;
        this.prepare();
    }

    private void prepare() {
        adjacencyAllocator.prepare();
        for (var propertiesAllocator : propertiesAllocators) {
            if (propertiesAllocator != null) {
                propertiesAllocator.prepare();
            }
        }
    }

    @Override
    public DeltaVarLongCompressor concurrentCopy() {
        return new DeltaVarLongCompressor(
            adjacencyBuilder,
            propertyBuilders,
            adjacencyBuilder.newAllocator(),
            Arrays
                .stream(propertyBuilders)
                .map(AdjacencyListBuilder::newAllocator)
                .toArray(AdjacencyListAllocator[]::new),
            adjacencyOffsetsFactory,
            adjacencyOffsets,
            propertyOffsets,
            noAggregation,
            aggregations
        );
    }

    @Override
    public boolean supportsProperties() {
        // TODO temporary until Geri does support properties
        return adjacencyBuilder instanceof TransientAdjacencyListBuilder;
    }

    @Override
    public int compress(
        long nodeId,
        CompressedLongArray values,
        LongArrayBuffer buffer
    ) {
        if (values.hasWeights()) {
            return applyVariableDeltaEncodingWithWeights(nodeId, values, buffer);
        } else {
            return applyVariableDeltaEncodingWithoutWeights(nodeId, values, buffer);
        }
    }

    @Override
    public void flush() {
        adjacencyBuilder.flush();
        for (var propertyBuilder : propertyBuilders) {
            if (propertyBuilder != null) {
                propertyBuilder.flush();
            }
        }
    }

    @Override
    public AdjacencyListsWithProperties build() {
        AdjacencyOffsets adjacencyOffsets = offsetPagesIntoOffsets(this.adjacencyOffsets);

        var builder = ImmutableAdjacencyListsWithProperties
            .builder()
            .adjacency(new DvlCompressionResult(adjacencyOffsets, adjacencyBuilder.build()));

        for (int i = 0; i < propertyBuilders.length; i++) {
            var compressedProps = new DvlCompressionResult(
                offsetPagesIntoOffsets(propertyOffsets[i]),
                propertyBuilders[i].build()
            );
            builder.addProperty(compressedProps);
        }

        return builder.build();
    }

    @Override
    public void close() {
        adjacencyAllocator.close();
        for (var propertiesAllocator : propertiesAllocators) {
            if (propertiesAllocator != null) {
                propertiesAllocator.close();
            }
        }
    }

    private int applyVariableDeltaEncodingWithoutWeights(
        long nodeId,
        CompressedLongArray array,
        LongArrayBuffer buffer
    ) {
        byte[] storage = array.storage();
        AdjacencyCompression.copyFrom(buffer, array);
        int degree = AdjacencyCompression.applyDeltaEncoding(buffer, aggregations[0]);
        int requiredBytes = AdjacencyCompression.compress(buffer, storage);

        long address = copyIds(storage, requiredBytes, degree);
        this.adjacencyOffsets.set(nodeId, address);

        array.release();

        return degree;
    }

    private int applyVariableDeltaEncodingWithWeights(
        long nodeId,
        CompressedLongArray array,
        LongArrayBuffer buffer
    ) {
        byte[] semiCompressedBytesDuringLoading = array.storage();
        long[][] uncompressedWeightsPerProperty = array.weights();

        // uncompressed semiCompressed into full uncompressed long[] (in buffer)
        // ordered by whatever order they've been read
        AdjacencyCompression.copyFrom(buffer, array);
        // buffer contains uncompressed, unsorted target list

        int degree = AdjacencyCompression.applyDeltaEncoding(
            buffer,
            uncompressedWeightsPerProperty,
            aggregations,
            noAggregation
        );
        // targets are sorted and delta encoded
        // buffer contains sorted target list
        // values are delta encoded except for the first one
        // values are still uncompressed

        int requiredBytes = AdjacencyCompression.compress(buffer, semiCompressedBytesDuringLoading);
        // values are now vlong encoded in the array storage (semiCompressed)

        var address = copyIds(semiCompressedBytesDuringLoading, requiredBytes, degree);
        // values are in the final adjacency list

        copyProperties(uncompressedWeightsPerProperty, degree, nodeId, propertyOffsets);

        this.adjacencyOffsets.set(nodeId, address);
        array.release();

        return degree;
    }

    private long copyIds(byte[] targets, int requiredBytes, int degree) {
        // sizeOf(degree) + compression bytes
        var slice = adjacencyAllocator.allocate(Integer.BYTES + requiredBytes);
        slice.writeInt(degree);
        slice.insert(targets, 0, requiredBytes);
        return slice.address();
    }

    private void copyProperties(long[][] properties, int degree, long nodeId, HugeLongArray[] offsets) {
        for (int i = 0; i < properties.length; i++) {
            long[] property = properties[i];
            var propertiesAllocator = propertiesAllocators[i];
            long address = copyProperties(property, degree, propertiesAllocator);
            offsets[i].set(nodeId, address);
        }
    }

    private long copyProperties(long[] properties, int degree, AdjacencyListAllocator propertiesAllocator) {
        int requiredBytes = degree * Long.BYTES;
        var slice = propertiesAllocator.allocate(Integer.BYTES /* degree */ + requiredBytes);
        slice.writeInt(degree);
        int offset = slice.offset();
        ByteBuffer
            .wrap(slice.page(), offset, requiredBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer()
            .put(properties, 0, degree);
        slice.bytesWritten(requiredBytes);
        return slice.address();
    }

    private AdjacencyOffsets offsetPagesIntoOffsets(HugeLongArray offsets) {
        long[][] pages = new long[offsets.pages()][];
        int pageIndex = 0;
        try (var cursor = offsets.initCursor(offsets.newCursor())) {
            while (cursor.next()) {
                pages[pageIndex++] = cursor.array;
            }
        }
        return adjacencyOffsetsFactory.newOffsets(pages);
    }

    private static final class DvlCompressionResult implements CompressedTopology, CompressedProperties {
        final AdjacencyOffsets offsets;
        final AdjacencyList adjacency;

        private DvlCompressionResult(AdjacencyOffsets offsets, AdjacencyList adjacency) {
            this.offsets = offsets;
            this.adjacency = adjacency;
        }

        @Override
        public AdjacencyOffsets adjacencyOffsets() {
            return offsets;
        }

        @Override
        public AdjacencyList adjacencyList() {
            return adjacency;
        }
    }
}
