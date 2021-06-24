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

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyProperties;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressor;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorBlueprint;
import org.neo4j.graphalgo.core.compress.AdjacencyCompressorFactory;
import org.neo4j.graphalgo.core.compress.AdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.graphalgo.core.compress.LongArrayBuffer;
import org.neo4j.graphalgo.core.utils.AscendingLongComparator;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.stream.Stream;

public final class RawCompressor implements AdjacencyCompressor {

    public enum Factory implements AdjacencyCompressorFactory<long[], long[]> {
        INSTANCE;

        @Override
        public AdjacencyCompressorBlueprint create(
            long nodeCount,
            CsrListBuilderFactory<long[], ? extends AdjacencyList, long[], ? extends AdjacencyProperties> csrListBuilderFactory,
            PropertyMappings propertyMappings,
            Aggregation[] aggregations,
            boolean noAggregation,
            AllocationTracker tracker
        ) {
            @SuppressWarnings("unchecked")
            CsrListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders = new CsrListBuilder[propertyMappings.numberOfMappings()];
            Arrays.setAll(propertyBuilders, i -> csrListBuilderFactory.newAdjacencyPropertiesBuilder());

            return new Blueprint(
                csrListBuilderFactory.newAdjacencyListBuilder(),
                propertyBuilders,
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
        private final CsrListBuilder<long[], ? extends AdjacencyList> adjacencyBuilder;
        private final CsrListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders;
        private final HugeIntArray adjacencyDegrees;
        private final HugeLongArray adjacencyOffsets;
        private final HugeLongArray[] propertyOffsets;
        private final boolean noAggregation;
        private final Aggregation[] aggregations;

        private Blueprint(
            CsrListBuilder<long[], ? extends AdjacencyList> adjacencyBuilder,
            CsrListBuilder<long[], ? extends AdjacencyProperties>[] propertyBuilders,
            HugeIntArray adjacencyDegrees,
            HugeLongArray adjacencyOffsets,
            HugeLongArray[] propertyOffsets,
            boolean noAggregation,
            Aggregation[] aggregations
        ) {
            this.adjacencyBuilder = adjacencyBuilder;
            this.propertyBuilders = propertyBuilders;
            this.adjacencyDegrees = adjacencyDegrees;
            this.adjacencyOffsets = adjacencyOffsets;
            this.propertyOffsets = propertyOffsets;
            this.noAggregation = noAggregation;
            this.aggregations = aggregations;
        }

        @Override
        @SuppressWarnings("unchecked")
        public RawCompressor createCompressor() {
            return new RawCompressor(
                adjacencyBuilder.newAllocator(),
                Arrays
                    .stream(propertyBuilders)
                    .map(CsrListBuilder::newAllocator)
                    .toArray(CsrListBuilder.Allocator[]::new),
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
            return adjacencyBuilder instanceof TransientUncompressedListBuilder;
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
            var adjacencyList = adjacencyBuilder.build(this.adjacencyDegrees, this.adjacencyOffsets);

            var builder = ImmutableAdjacencyListsWithProperties
                .builder()
                .adjacency(adjacencyList);

            for (int i = 0; i < propertyBuilders.length; i++) {
                var properties = propertyBuilders[i].build(this.adjacencyDegrees, propertyOffsets[i]);
                builder.addProperty(properties);
            }

            return builder.build();
        }
    }

    private final CsrListBuilder.Allocator<long[]> adjacencyAllocator;
    private final CsrListBuilder.Allocator<long[]>[] propertiesAllocators;
    private final HugeIntArray adjacencyDegrees;
    private final HugeLongArray adjacencyOffsets;
    private final HugeLongArray[] propertyOffsets;
    private final boolean noAggregation;
    private final Aggregation[] aggregations;

    private RawCompressor(
        CsrListBuilder.Allocator<long[]> adjacencyAllocator,
        CsrListBuilder.Allocator<long[]>[] propertiesAllocators,
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
            return withWeights(nodeId, values, buffer);
        } else {
            return withoutWeights(nodeId, values, buffer);
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

    private int withoutWeights(
        long nodeId,
        CompressedLongArray array,
        LongArrayBuffer buffer
    ) {
        // decompress target ids
        AdjacencyCompression.copyFrom(buffer, array);

        int degree = aggregate(buffer, aggregations[0]);

        long address = copy(buffer.buffer, degree, adjacencyAllocator);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, address);

        array.release();

        return degree;
    }

    private int aggregate(LongArrayBuffer targets, Aggregation aggregation) {
        var values = targets.buffer;
        var length = targets.length;

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

    private int aggregateWithWeights(LongArrayBuffer targets, long[][] weights, Aggregation[] aggregations) {
        var values = targets.buffer;
        var length = targets.length;

        int[] order = IndirectSort.mergesort(0, length, new AscendingLongComparator(values));

        long[] outValues = new long[length];
        long[][] outWeights = new long[weights.length][length];

        int firstSortIdx = order[0];
        long value = values[firstSortIdx];
        long delta;

        outValues[0] = value;
        for (int i = 0; i < weights.length; i++) {
            outWeights[i][0] = weights[i][firstSortIdx];
        }

        int in = 1, out = 1;

        if (noAggregation) {
            for (; in < length; ++in) {
                int sortIdx = order[in];

                for (int i = 0; i < weights.length; i++) {
                    outWeights[i][out] = weights[i][sortIdx];
                }

                outValues[out++] = values[sortIdx];
            }
        } else {
            for (; in < length; ++in) {
                int sortIdx = order[in];
                delta = values[sortIdx] - value;
                value = values[sortIdx];

                if (delta > 0L) {
                    for (int i = 0; i < weights.length; i++) {
                        outWeights[i][out] = weights[i][sortIdx];
                    }
                    outValues[out++] = value;
                } else {
                    for (int i = 0; i < weights.length; i++) {
                        Aggregation aggregation = aggregations[i];
                        int existingIdx = out - 1;
                        long[] outWeight = outWeights[i];
                        double existingWeight = Double.longBitsToDouble(outWeight[existingIdx]);
                        double newWeight = Double.longBitsToDouble(weights[i][sortIdx]);
                        newWeight = aggregation.merge(existingWeight, newWeight);
                        outWeight[existingIdx] = Double.doubleToLongBits(newWeight);
                    }
                }
            }
        }

        System.arraycopy(outValues, 0, values, 0, out);
        for (int i = 0; i < outWeights.length; i++) {
            System.arraycopy(outWeights[i], 0, weights[i], 0, out);
        }

        return out;

    }

    private int withWeights(
        long nodeId,
        CompressedLongArray array,
        LongArrayBuffer buffer
    ) {
        long[][] uncompressedWeights = array.weights();

        AdjacencyCompression.copyFrom(buffer, array);

        int degree = aggregateWithWeights(buffer, uncompressedWeights, aggregations);

        long address = copy(buffer.buffer, degree, adjacencyAllocator);

        copyProperties(uncompressedWeights, degree, nodeId, propertyOffsets);

        this.adjacencyDegrees.set(nodeId, degree);
        this.adjacencyOffsets.set(nodeId, address);
        array.release();

        return degree;
    }

    private void copyProperties(long[][] properties, int degree, long nodeId, HugeLongArray[] offsets) {
        for (int i = 0; i < properties.length; i++) {
            long[] property = properties[i];
            var propertiesAllocator = propertiesAllocators[i];
            long address = copy(property, degree, propertiesAllocator);
            offsets[i].set(nodeId, address);
        }
    }

    private long copy(long[] data, int degree, CsrListBuilder.Allocator<long[]> allocator) {
        return allocator.write(data, degree);
    }
}
