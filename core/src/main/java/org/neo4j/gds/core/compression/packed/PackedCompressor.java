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

import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.AdjacencyListsWithProperties;
import org.neo4j.gds.api.compress.ImmutableAdjacencyListsWithProperties;
import org.neo4j.gds.api.compress.LongArrayBuffer;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public final class PackedCompressor implements AdjacencyCompressor {

    static final int FLAGS = AdjacencyPacker.DELTA | AdjacencyPacker.SORT;

    private final HugeObjectArray<Compressed> adjacencies;
    private final Aggregation[] aggregations;
    private final boolean noAggregation;
    // TODO: only used for non-property case
    private final int flags;

    public static AdjacencyCompressorFactory factory(
        LongSupplier nodeCountSupplier,
        PropertyMappings propertyMappings,
        Aggregation[] aggregations,
        boolean noAggregation
    ) {
        return new Factory(nodeCountSupplier, propertyMappings, aggregations, noAggregation);
    }

    private PackedCompressor(HugeObjectArray<Compressed> adjacencies, Aggregation[] aggregations, boolean noAggregation) {
        this.adjacencies = adjacencies;
        this.aggregations = aggregations;
        this.noAggregation = noAggregation;

        // TODO: only used for non-property case
        this.flags = FLAGS | aggregations[0].ordinal();
    }

    @Override
    public int compress(
        long nodeId,
        byte[] targets,
        long[][] properties,
        int numberOfCompressedTargets,
        int compressedBytesSize,
        LongArrayBuffer buffer,
        ValueMapper mapper
    ) {
        Compressed compressed;
        if (properties != null) {
            compressed = packWithProperties(
                targets,
                properties,
                numberOfCompressedTargets,
                compressedBytesSize,
                buffer,
                mapper
            );
        } else {
            compressed = packWithoutProperties(
                targets,
                numberOfCompressedTargets,
                compressedBytesSize,
                buffer,
                mapper
            );
        }

        this.adjacencies.set(nodeId, compressed);
        return compressed.length();
    }

    private Compressed packWithProperties(
        byte[] semiCompressedBytesDuringLoading,
        long[][] uncompressedPropertiesPerProperty,
        int numberOfCompressedTargets,
        int compressedByteSize,
        LongArrayBuffer buffer,
        ValueMapper mapper
    ) {
        AdjacencyCompression.zigZagUncompressFrom(
            buffer,
            semiCompressedBytesDuringLoading,
            numberOfCompressedTargets,
            compressedByteSize,
            mapper
        );

        long[] targets = buffer.buffer;
        int targetsLength = buffer.length;

        return AdjacencyPacker.compressWithProperties(
            targets,
            uncompressedPropertiesPerProperty,
            targetsLength,
            aggregations,
            noAggregation
        );
    }

    private Compressed packWithoutProperties(
        byte[] semiCompressedBytesDuringLoading,
        int numberOfCompressedTargets,
        int compressedByteSize,
        LongArrayBuffer buffer,
        ValueMapper mapper
    ) {
        AdjacencyCompression.zigZagUncompressFrom(
            buffer,
            semiCompressedBytesDuringLoading,
            numberOfCompressedTargets,
            compressedByteSize,
            mapper
        );

        long[] targets = buffer.buffer;
        int targetsLength = buffer.length;

        return AdjacencyPacker.compress(targets, 0, targetsLength, this.flags);
    }

    @Override
    public void close() {

    }

    static class Factory implements AdjacencyCompressorFactory {

        private final LongSupplier nodeCountSupplier;
        private final PropertyMappings propertyMappings;
        private final Aggregation[] aggregations;
        private final boolean noAggregation;

        private final LongAdder relationshipCounter;

        private HugeObjectArray<Compressed> adjacencies;

        Factory(
            LongSupplier nodeCountSupplier,
            PropertyMappings propertyMappings,
            Aggregation[] aggregations,
            boolean noAggregation
        ) {
            this.nodeCountSupplier = nodeCountSupplier;
            this.propertyMappings = propertyMappings;
            this.aggregations = aggregations;
            this.noAggregation = noAggregation;
            this.relationshipCounter = new LongAdder();
        }

        @Override
        public void init() {
            long nodeCount = this.nodeCountSupplier.getAsLong();
            this.adjacencies = HugeObjectArray.newArray(Compressed.class, nodeCount);
        }

        @Override
        public AdjacencyCompressor createCompressor() {
            return new PackedCompressor(this.adjacencies, this.aggregations, this.noAggregation);
        }

        @Override
        public LongAdder relationshipCounter() {
            return this.relationshipCounter;
        }

        @Override
        public AdjacencyListsWithProperties build() {
            var adjacency = new PackedAdjacencyList(this.adjacencies);

            var builder = ImmutableAdjacencyListsWithProperties.builder()
                .adjacency(adjacency)
                .relationshipCount(this.relationshipCounter.longValue());

            var mappings = propertyMappings.mappings();
            for (int i = 0; i < mappings.size(); i++) {
                var property = new PackedPropertyList(this.adjacencies, i);
                builder.addProperty(property);
            }

            return builder.build();
        }
    }
}
