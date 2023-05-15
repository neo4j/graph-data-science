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
package org.neo4j.gds.core.compression.packed;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.AdjacencyListBuilderFactory;
import org.neo4j.gds.api.compress.LongArrayBuffer;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AbstractAdjacencyCompressorFactory;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.utils.GdsSystemProperties;

import java.util.Arrays;
import java.util.function.LongSupplier;

public final class PackedCompressor implements AdjacencyCompressor {

    public static AdjacencyCompressorFactory factory(
        LongSupplier nodeCountSupplier,
        AdjacencyListBuilderFactory<Address, ? extends AdjacencyList, long[], ? extends AdjacencyProperties> adjacencyListBuilderFactory,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        AdjacencyListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders = new AdjacencyListBuilder[propertyMappings.numberOfMappings()];
        Arrays.setAll(propertyBuilders, i -> adjacencyListBuilderFactory.newAdjacencyPropertiesBuilder());

        return new Factory(
            nodeCountSupplier,
            adjacencyListBuilderFactory.newAdjacencyListBuilder(),
            propertyBuilders,
            noAggregation,
            aggregations
        );
    }

    static class Factory extends AbstractAdjacencyCompressorFactory<Address, long[]> {

        Factory(
            LongSupplier nodeCountSupplier,
            AdjacencyListBuilder<Address, ? extends AdjacencyList> adjacencyBuilder,
            AdjacencyListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders,
            boolean noAggregation,
            Aggregation[] aggregations
        ) {
            super(
                nodeCountSupplier,
                adjacencyBuilder,
                propertyBuilders,
                noAggregation,
                aggregations
            );
        }

        @Override
        protected AdjacencyCompressor createCompressorFromInternalState(
            AdjacencyListBuilder<Address, ? extends AdjacencyList> adjacencyBuilder,
            AdjacencyListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders,
            boolean noAggregation,
            Aggregation[] aggregations,
            HugeIntArray adjacencyDegrees,
            HugeLongArray adjacencyOffsets,
            HugeLongArray propertyOffsets
        ) {
            AdjacencyListBuilder.Allocator<long[]> firstAllocator;
            AdjacencyListBuilder.PositionalAllocator<long[]>[] otherAllocators;

            if (propertyBuilders.length > 0) {
                firstAllocator = propertyBuilders[0].newAllocator();
                //noinspection unchecked
                otherAllocators = new AdjacencyListBuilder.PositionalAllocator[propertyBuilders.length - 1];
                Arrays.setAll(
                    otherAllocators,
                    i -> propertyBuilders[i + 1].newPositionalAllocator()
                );
            } else {
                firstAllocator = null;
                otherAllocators = null;
            }

            return new PackedCompressor(
                adjacencyBuilder.newAllocator(),
                firstAllocator,
                otherAllocators,
                adjacencyDegrees,
                adjacencyOffsets,
                propertyOffsets,
                noAggregation,
                aggregations
            );
        }
    }

    private final AdjacencyListBuilder.Allocator<Address> adjacencyAllocator;
    private final @Nullable AdjacencyListBuilder.Allocator<long[]> firstPropertyAllocator;
    private final AdjacencyListBuilder.PositionalAllocator<long[]> @Nullable [] otherPropertyAllocators;
    private final HugeIntArray adjacencyDegrees;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    private final LongArrayBuffer buffer;
    private final ModifiableSlice<Address> adjacencySlice;
    private final ModifiableSlice<long[]> propertySlice;
    private final MutableInt degree;

    private final boolean packed;

    private PackedCompressor(
        AdjacencyListBuilder.Allocator<Address> adjacencyAllocator,
        @Nullable AdjacencyListBuilder.Allocator<long[]> firstPropertyAllocator,
        AdjacencyListBuilder.PositionalAllocator<long[]> @Nullable [] otherPropertyAllocators,
        HugeIntArray adjacencyDegrees,
        HugeLongArray adjacencyOffsets,
        HugeLongArray propertyOffsets,
        boolean noAggregation,
        Aggregation[] aggregations
    ) {
        this.adjacencyAllocator = adjacencyAllocator;
        this.firstPropertyAllocator = firstPropertyAllocator;
        this.otherPropertyAllocators = otherPropertyAllocators;
        this.adjacencyDegrees = adjacencyDegrees;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyOffsets = propertyOffsets;
        this.noAggregation = noAggregation;
        this.aggregations = aggregations;

        this.buffer = new LongArrayBuffer();
        this.adjacencySlice = ModifiableSlice.create();
        this.propertySlice = ModifiableSlice.create();
        this.degree = new MutableInt(0);

        this.packed = GdsSystemProperties.PACKED_TAIL_COMPRESSION == GdsSystemProperties.PackedTailCompression.Packed;
    }

    @Override
    public int compress(
        long nodeId,
        byte[][] targets,
        long[][] properties,
        int numberOfCompressedTargets,
        int compressedBytesSize,
        ValueMapper mapper
    ) {
        if (properties != null) {
            return packWithProperties(
                nodeId,
                targets,
                properties,
                numberOfCompressedTargets,
                compressedBytesSize,
                mapper
            );
        } else {
            return packWithoutProperties(
                nodeId,
                targets,
                numberOfCompressedTargets,
                compressedBytesSize,
                mapper
            );
        }
    }

    private int packWithProperties(
        long nodeId,
        byte[][] semiCompressedBytesDuringLoading,
        long[][] uncompressedPropertiesPerProperty,
        int numberOfCompressedTargets,
        int compressedByteSize,
        ValueMapper mapper
    ) {
        AdjacencyCompression.zigZagUncompressFrom(
            this.buffer,
            semiCompressedBytesDuringLoading,
            numberOfCompressedTargets,
            compressedByteSize,
            mapper
        );

        long[] targets = this.buffer.buffer;
        int targetsLength = this.buffer.length;
        long offset;
        if (this.packed) {
            offset = AdjacencyPacker.compressWithPropertiesWithPackedTail(
                this.adjacencyAllocator,
                this.adjacencySlice,
                targets,
                uncompressedPropertiesPerProperty,
                targetsLength,
                this.aggregations,
                this.noAggregation,
                this.degree
            );
        } else {
            offset = AdjacencyPacker.compressWithPropertiesWithVarLongTail(
                this.adjacencyAllocator,
                this.adjacencySlice,
                targets,
                uncompressedPropertiesPerProperty,
                targetsLength,
                this.aggregations,
                this.noAggregation,
                this.degree
            );
        }

        int degree = this.degree.intValue();

        copyProperties(uncompressedPropertiesPerProperty, degree, nodeId);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, offset);

        return degree;
    }

    private int packWithoutProperties(
        long nodeId,
        byte[][] semiCompressedBytesDuringLoading,
        int numberOfCompressedTargets,
        int compressedByteSize,
        ValueMapper mapper
    ) {
        AdjacencyCompression.zigZagUncompressFrom(
            this.buffer,
            semiCompressedBytesDuringLoading,
            numberOfCompressedTargets,
            compressedByteSize,
            mapper
        );

        long[] targets = this.buffer.buffer;
        int targetsLength = this.buffer.length;

        long offset;

        if (this.packed) {
            offset = AdjacencyPacker.compressWithPackedTail(
                this.adjacencyAllocator,
                this.adjacencySlice,
                targets,
                targetsLength,
                this.aggregations[0],
                this.degree
            );
        } else {
            offset = AdjacencyPacker.compressWithVarLongTail(
                this.adjacencyAllocator,
                this.adjacencySlice,
                targets,
                targetsLength,
                this.aggregations[0],
                this.degree
            );
        }

        int degree = this.degree.intValue();

        this.adjacencyOffsets.set(nodeId, offset);
        this.adjacencyDegrees.set(nodeId, degree);

        return degree;
    }

    private void copyProperties(long[][] properties, int degree, long nodeId) {
        assert this.firstPropertyAllocator != null;
        assert this.otherPropertyAllocators != null;

        var slice = this.propertySlice;
        long address = this.firstPropertyAllocator.allocate(degree, slice);
        System.arraycopy(properties[0], 0, slice.slice(), slice.offset(), degree);

        for (int i = 1; i < properties.length; i++) {
            this.otherPropertyAllocators[i - 1].writeAt(address, properties[i], degree);
        }

        this.propertyOffsets.set(nodeId, address);
    }

    @Override
    public void close() {

    }
}
