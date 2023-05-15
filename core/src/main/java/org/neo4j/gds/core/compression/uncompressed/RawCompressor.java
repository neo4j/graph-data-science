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
package org.neo4j.gds.core.compression.uncompressed;

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListBuilder;
import org.neo4j.gds.api.compress.AdjacencyListBuilderFactory;
import org.neo4j.gds.api.compress.ModifiableSlice;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AbstractAdjacencyCompressorFactory;
import org.neo4j.gds.core.utils.AscendingLongComparator;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.function.LongSupplier;

public final class RawCompressor implements AdjacencyCompressor {

    public static AdjacencyCompressorFactory factory(
        LongSupplier nodeCountSupplier,
        AdjacencyListBuilderFactory<long[], ? extends AdjacencyList, long[], ? extends AdjacencyProperties> adjacencyListBuilderFactory,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        @SuppressWarnings("unchecked")
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

    private static final class Factory extends AbstractAdjacencyCompressorFactory<long[], long[]> {

        Factory(
            LongSupplier nodeCountSupplier,
            AdjacencyListBuilder<long[], ? extends AdjacencyList> adjacencyBuilder,
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
            AdjacencyListBuilder<long[], ? extends AdjacencyList> adjacencyBuilder,
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

            return new RawCompressor(
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

    private final AdjacencyListBuilder.Allocator<long[]> adjacencyAllocator;
    private final @Nullable AdjacencyListBuilder.Allocator<long[]> firstPropertyAllocator;
    private final AdjacencyListBuilder.PositionalAllocator<long[]> @Nullable [] otherPropertyAllocators;
    private final HugeIntArray adjacencyDegrees;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    private final ModifiableSlice<long[]> slice;

    private RawCompressor(
        AdjacencyListBuilder.Allocator<long[]> adjacencyAllocator,
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

        this.slice = ModifiableSlice.create();
    }

    @Override
    public int compress(long nodeId, long[] targets, long[][] properties, int degree) {
        if (properties != null) {
            return withProperties(
                nodeId,
                targets,
                properties,
                degree
            );
        } else {
            return withoutProperties(
                nodeId, targets, degree
            );
        }
    }

    @Override
    public void close() {
        this.adjacencyAllocator.close();
        if (this.firstPropertyAllocator != null) {
            this.firstPropertyAllocator.close();
        }
        if (this.otherPropertyAllocators != null) {
            for (var otherPropertyAllocator : this.otherPropertyAllocators) {
                // TODO: Do we need this null check here?
                if (otherPropertyAllocator != null) {
                    otherPropertyAllocator.close();
                }
            }
        }
    }

    private int withoutProperties(long nodeId, long[] targets, int degree) {
        degree = aggregate(targets, degree, this.aggregations[0]);

        long address = copy(targets, degree);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, address);

        return degree;
    }

    private int aggregate(long[] values, int length, Aggregation aggregation) {
        Arrays.sort(values, 0, length);

        if (aggregation == Aggregation.NONE) {
            return length;
        }

        int read = 1, write = 1;

        for (; read < length; ++read) {
            long value = values[read];
            if (value != values[read - 1]) {
                values[write++] = value;
            }
        }

        return write;
    }

    private int aggregateWithProperties(long[] values, int length, long[][] properties, Aggregation[] aggregations) {
        int[] order = IndirectSort.mergesort(0, length, new AscendingLongComparator(values));

        long[] outValues = new long[length];
        long[][] outProperties = new long[properties.length][length];

        int firstSortIdx = order[0];
        long value = values[firstSortIdx];
        long delta;

        outValues[0] = value;
        for (int i = 0; i < properties.length; i++) {
            outProperties[i][0] = properties[i][firstSortIdx];
        }

        int in = 1, out = 1;

        if (this.noAggregation) {
            for (; in < length; ++in) {
                int sortIdx = order[in];

                for (int i = 0; i < properties.length; i++) {
                    outProperties[i][out] = properties[i][sortIdx];
                }

                outValues[out++] = values[sortIdx];
            }
        } else {
            for (; in < length; ++in) {
                int sortIdx = order[in];
                delta = values[sortIdx] - value;
                value = values[sortIdx];

                if (delta > 0L) {
                    for (int i = 0; i < properties.length; i++) {
                        outProperties[i][out] = properties[i][sortIdx];
                    }
                    outValues[out++] = value;
                } else {
                    for (int i = 0; i < properties.length; i++) {
                        Aggregation aggregation = aggregations[i];
                        int existingIdx = out - 1;
                        long[] outProperty = outProperties[i];
                        double existingProperty = Double.longBitsToDouble(outProperty[existingIdx]);
                        double newProperty = Double.longBitsToDouble(properties[i][sortIdx]);
                        newProperty = aggregation.merge(existingProperty, newProperty);
                        outProperty[existingIdx] = Double.doubleToLongBits(newProperty);
                    }
                }
            }
        }

        System.arraycopy(outValues, 0, values, 0, out);
        for (int i = 0; i < outProperties.length; i++) {
            System.arraycopy(outProperties[i], 0, properties[i], 0, out);
        }

        return out;

    }

    private int withProperties(
        long nodeId,
        long[] targets,
        long[][] uncompressedProperties,
        int degree
    ) {
        degree = aggregateWithProperties(targets, degree, uncompressedProperties, this.aggregations);

        long address = copy(targets, degree);

        copyProperties(uncompressedProperties, degree, nodeId);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, address);

        return degree;
    }

    private long copy(long[] data, int degree) {
        var slice = this.slice;
        long address = this.adjacencyAllocator.allocate(degree, slice);
        System.arraycopy(data, 0, slice.slice(), slice.offset(), degree);
        return address;
    }

    private void copyProperties(long[][] properties, int degree, long nodeId) {
        assert this.firstPropertyAllocator != null;
        assert this.otherPropertyAllocators != null;

        var slice = this.slice;
        long address = this.firstPropertyAllocator.allocate(degree, slice);
        System.arraycopy(properties[0], 0, slice.slice(), slice.offset(), degree);

        for (int i = 1; i < properties.length; i++) {
            this.otherPropertyAllocators[i - 1].writeAt(address, properties[i], degree);
        }

        this.propertyOffsets.set(nodeId, address);
    }
}
