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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.core.compress.LongArrayBuffer;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.function.LongSupplier;

public final class DeltaVarLongCompressor implements AdjacencyCompressor {

    private final AdjacencyListBuilder.Allocator<byte[]> adjacencyAllocator;
    private final AdjacencyListBuilder.Allocator<long[]>[] propertiesAllocators;
    private final HugeIntArray adjacencyDegrees;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray[] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    public static AdjacencyCompressorFactory factory(
        LongSupplier nodeCountSupplier,
        AdjacencyListBuilderFactory<byte[], ? extends AdjacencyList, long[], ? extends AdjacencyProperties> adjacencyListBuilderFactory,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation,
        AllocationTracker allocationTracker
    ) {
        @SuppressWarnings("unchecked")
        AdjacencyListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders = new AdjacencyListBuilder[propertyMappings.numberOfMappings()];
        Arrays.setAll(propertyBuilders, i -> adjacencyListBuilderFactory.newAdjacencyPropertiesBuilder());

        return new Factory(
            nodeCountSupplier,
            adjacencyListBuilderFactory.newAdjacencyListBuilder(),
            propertyBuilders,
            noAggregation,
            aggregations,
            allocationTracker
        );
    }

    private DeltaVarLongCompressor(
        AdjacencyListBuilder.Allocator<byte[]> adjacencyAllocator,
        AdjacencyListBuilder.Allocator<long[]>[] propertiesAllocators,
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
        return adjacencyAllocator.write(targets, requiredBytes);
    }

    private void copyProperties(long[][] properties, int degree, long nodeId, HugeLongArray[] offsets) {
        for (int i = 0; i < properties.length; i++) {
            long[] property = properties[i];
            var propertiesAllocator = propertiesAllocators[i];
            long address = propertiesAllocator.write(property, degree);
            offsets[i].set(nodeId, address);
        }
    }

    private static final class Factory extends AbstractAdjacencyCompressorFactory<byte[], long[]> {

        Factory(
            LongSupplier nodeCountSupplier,
            AdjacencyListBuilder<byte[], ? extends AdjacencyList> adjacencyBuilder,
            AdjacencyListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders,
            boolean noAggregation,
            Aggregation[] aggregations,
            AllocationTracker allocationTracker
        ) {
            super(
                nodeCountSupplier,
                adjacencyBuilder,
                propertyBuilders,
                noAggregation,
                aggregations,
                allocationTracker
            );
        }

        @Override
        @SuppressWarnings("unchecked")
        public DeltaVarLongCompressor createCompressor() {
            return new DeltaVarLongCompressor(
                adjacencyBuilder.newAllocator(),
                Arrays
                    .stream(propertyBuilders)
                    .map(AdjacencyListBuilder::newAllocator)
                    .toArray(AdjacencyListBuilder.Allocator[]::new),
                adjacencyDegrees,
                adjacencyOffsets,
                propertyOffsets,
                noAggregation,
                aggregations
            );
        }
    }
}
