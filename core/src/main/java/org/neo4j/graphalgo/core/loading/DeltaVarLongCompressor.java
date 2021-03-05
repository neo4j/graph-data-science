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

import org.neo4j.graphalgo.api.AdjacencyDegrees;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressor;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorBlueprint;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorFactory;
import org.neo4j.graphalgo.core.compress.AdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.LongArrayBuffer;
import org.neo4j.graphalgo.core.compress.CompressedProperties;
import org.neo4j.graphalgo.core.compress.CompressedTopology;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.stream.Stream;

public final class DeltaVarLongCompressor implements AdjacencyCompressor {

    public static final class Factory implements AdjacencyCompressorFactory {

        private final AdjacencyDegreesFactory adjacencyDegreesFactory;
        private final AdjacencyOffsetsFactory adjacencyOffsetsFactory;

        public Factory(
            AdjacencyDegreesFactory adjacencyDegreesFactory,
            AdjacencyOffsetsFactory adjacencyOffsetsFactory
        ) {
            this.adjacencyDegreesFactory = adjacencyDegreesFactory;
            this.adjacencyOffsetsFactory = adjacencyOffsetsFactory;
        }

        @Override
        public AdjacencyCompressorBlueprint create(
            long nodeCount,
            AdjacencyListBuilder adjacencyBuilder,
            AdjacencyListBuilder[] propertyBuilders,
            Aggregation[] aggregations,
            boolean noAggregation,
            AllocationTracker tracker
        ) {
            return new Blueprint(
                adjacencyBuilder,
                propertyBuilders,
                adjacencyDegreesFactory,
                adjacencyOffsetsFactory,
                HugeIntArray.newArray(nodeCount, tracker),
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

    private static final class Blueprint implements AdjacencyCompressorBlueprint {
        private final AdjacencyListBuilder adjacencyBuilder;
        private final AdjacencyListBuilder[] propertyBuilders;
        private final AdjacencyDegreesFactory adjacencyDegreesFactory;
        private final AdjacencyOffsetsFactory adjacencyOffsetsFactory;
        private final HugeIntArray adjacencyDegrees;
        private final HugeLongArray adjacencyOffsets;
        private final HugeLongArray[] propertyOffsets;
        private final boolean noAggregation;
        private final Aggregation[] aggregations;

        private Blueprint(
            AdjacencyListBuilder adjacencyBuilder,
            AdjacencyListBuilder[] propertyBuilders,
            AdjacencyDegreesFactory adjacencyDegreesFactory,
            AdjacencyOffsetsFactory adjacencyOffsetsFactory,
            HugeIntArray adjacencyDegrees,
            HugeLongArray adjacencyOffsets,
            HugeLongArray[] propertyOffsets,
            boolean noAggregation,
            Aggregation[] aggregations
        ) {
            this.adjacencyBuilder = adjacencyBuilder;
            this.propertyBuilders = propertyBuilders;
            this.adjacencyDegreesFactory = adjacencyDegreesFactory;
            this.adjacencyOffsetsFactory = adjacencyOffsetsFactory;
            this.adjacencyDegrees = adjacencyDegrees;
            this.adjacencyOffsets = adjacencyOffsets;
            this.propertyOffsets = propertyOffsets;
            this.noAggregation = noAggregation;
            this.aggregations = aggregations;
        }

        @Override
        public DeltaVarLongCompressor createCompressor() {
            return new DeltaVarLongCompressor(
                adjacencyBuilder.newAllocator(),
                Arrays
                    .stream(propertyBuilders)
                    .map(AdjacencyListBuilder::newAllocator)
                    .toArray(AdjacencyListAllocator[]::new),
                adjacencyDegrees,
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
            AdjacencyDegrees adjacencyDegrees = degreePagesIntoDegrees(this.adjacencyDegrees);
            AdjacencyOffsets adjacencyOffsets = offsetPagesIntoOffsets(this.adjacencyOffsets);

            var builder = ImmutableAdjacencyListsWithProperties
                .builder()
                .adjacency(new DvlCompressionResult(
                    adjacencyDegrees,
                    adjacencyOffsets,
                    adjacencyBuilder.build()
                ));

            for (int i = 0; i < propertyBuilders.length; i++) {
                var compressedProps = new DvlCompressionResult(
                    adjacencyDegrees,
                    offsetPagesIntoOffsets(propertyOffsets[i]),
                    propertyBuilders[i].build()
                );
                builder.addProperty(compressedProps);
            }

            return builder.build();
        }

        private AdjacencyDegrees degreePagesIntoDegrees(HugeIntArray degrees) {
            return adjacencyDegreesFactory.newDegrees(degrees);
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
    }

    private final AdjacencyListAllocator adjacencyAllocator;
    private final AdjacencyListAllocator[] propertiesAllocators;
    private final HugeIntArray adjacencyDegrees;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray[] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    private DeltaVarLongCompressor(
        AdjacencyListAllocator adjacencyAllocator,
        AdjacencyListAllocator[] propertiesAllocators,
        HugeIntArray adjacencyDegrees,
        HugeLongArray adjacencyOffsets,
        HugeLongArray[] propertyOffsets,
        boolean noAggregation,
        Aggregation[] aggregations
    ) {
        this.adjacencyAllocator = adjacencyAllocator;
        this.propertiesAllocators = propertiesAllocators;
        this.adjacencyDegrees = adjacencyDegrees;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.noAggregation = noAggregation;
        this.aggregations = aggregations;
        adjacencyAllocator.prepare();
        for (var propertiesAllocator : propertiesAllocators) {
            if (propertiesAllocator != null) {
                propertiesAllocator.prepare();
            }
        }
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

        long address = copyIds(storage, requiredBytes);

        this.adjacencyDegrees.set(nodeId, degree);
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

        // decompress semiCompressed into full uncompressed long[] (in buffer)
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

        var address = copyIds(semiCompressedBytesDuringLoading, requiredBytes);
        // values are in the final adjacency list

        copyProperties(uncompressedWeightsPerProperty, degree, nodeId, propertyOffsets);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, address);
        array.release();

        return degree;
    }

    private long copyIds(byte[] targets, int requiredBytes) {
        // sizeOf(degree) + compression bytes
        var slice = adjacencyAllocator.allocate(requiredBytes);
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
        var slice = propertiesAllocator.allocate(requiredBytes);
        int offset = slice.offset();
        ByteBuffer
            .wrap(slice.page(), offset, requiredBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .asLongBuffer()
            .put(properties, 0, degree);
        slice.bytesWritten(requiredBytes);
        return slice.address();
    }

    private static final class DvlCompressionResult implements CompressedTopology, CompressedProperties {
        final AdjacencyDegrees degrees;
        final AdjacencyOffsets offsets;
        final AdjacencyList adjacency;

        private DvlCompressionResult(
            AdjacencyDegrees degrees,
            AdjacencyOffsets offsets,
            AdjacencyList adjacency
        ) {
            this.degrees = degrees;
            this.offsets = offsets;
            this.adjacency = adjacency;
        }

        @Override
        public AdjacencyDegrees adjacencyDegrees() {
            return degrees;
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
