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

import org.apache.commons.lang3.mutable.MutableLong;
import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.compress.AdjacencyCompressor;
import org.neo4j.gds.api.compress.AdjacencyCompressorFactory;
import org.neo4j.gds.api.compress.LongArrayBuffer;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compression.common.ZigZagLongDecoding;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;

import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.preAggregate;
import static org.neo4j.gds.mem.BitUtil.ceilDiv;
import static org.neo4j.gds.mem.MemoryUsage.sizeOfObjectArray;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

/**
 * Wraps a paged representation of {@link org.neo4j.gds.core.loading.ChunkedAdjacencyLists}s
 * which store the target ids for each node during import.
 *
 * An instance of this class exists exactly once per relationship type and has the following
 * responsibilities:
 *
 * <ul>
 *     <li>Receives raw relationship records from relationship batch buffers via {@link org.neo4j.gds.core.loading.SingleTypeRelationshipImporter}</li>
 *     <li>Compresses raw records into compressed long arrays</li>
 *     <li>Creates tasks that write compressed long arrays into the final adjacency list using a specific compressor</li>
 * </ul>
 */
@Value.Style(typeBuilder = "AdjacencyBufferBuilder")
public final class AdjacencyBuffer {

    private final AdjacencyCompressorFactory adjacencyCompressorFactory;
    private final ThreadLocalRelationshipsBuilder[] localBuilders;
    private final ChunkedAdjacencyLists[] chunkedAdjacencyLists;
    private final AdjacencyBufferPaging paging;
    private final LongAdder relationshipCounter;
    private final int[] propertyKeyIds;
    private final double[] defaultValues;
    private final Aggregation[] aggregations;
    private final boolean atLeastOnePropertyToLoad;

    public static MemoryEstimation memoryEstimation(
        RelationshipType relationshipType,
        int propertyCount,
        boolean undirected
    ) {
        return MemoryEstimations.setup("", (dimensions, concurrency) -> {
            long nodeCount = dimensions.nodeCount();
            long relCountForType = dimensions
                .relationshipCounts()
                .getOrDefault(relationshipType, dimensions.relCountUpperBound());
            long relCount = undirected ? relCountForType * 2 : relCountForType;
            long avgDegree = (nodeCount > 0) ? ceilDiv(relCount, nodeCount) : 0L;
            return memoryEstimation(avgDegree, nodeCount, propertyCount, concurrency);
        });
    }

    public static MemoryEstimation memoryEstimation(
        long avgDegree,
        long nodeCount,
        int propertyCount,
        int concurrency
    ) {
        var importSizing = ImportSizing.of(concurrency, nodeCount);
        var numberOfPages = importSizing.numberOfPages();
        //noinspection OptionalGetWithoutIsPresent -- pageSize is defined because we have a nodeCount
        var pageSize = importSizing.pageSize().getAsInt();

        return MemoryEstimations
            .builder(AdjacencyBuffer.class)
            .fixed("ChunkedAdjacencyLists pages", sizeOfObjectArray(numberOfPages))
            .add(
                "ChunkedAdjacencyLists",
                ChunkedAdjacencyLists.memoryEstimation(avgDegree, pageSize, propertyCount).times(numberOfPages)
            )
            .build();
    }

    @Builder.Factory
    public static AdjacencyBuffer of(
        SingleTypeRelationshipImporter.ImportMetaData importMetaData,
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        ImportSizing importSizing
    ) {
        var numPages = importSizing.numberOfPages();
        var pageSize = importSizing.pageSize();

        ThreadLocalRelationshipsBuilder[] localBuilders = new ThreadLocalRelationshipsBuilder[numPages];
        ChunkedAdjacencyLists[] compressedAdjacencyLists = new ChunkedAdjacencyLists[numPages];

        for (int page = 0; page < numPages; page++) {
            compressedAdjacencyLists[page] = ChunkedAdjacencyLists.of(
                importMetaData.propertyKeyIds().length,
                pageSize.orElse(0)
            );
            localBuilders[page] = new ThreadLocalRelationshipsBuilder(adjacencyCompressorFactory);
        }

        boolean atLeastOnePropertyToLoad = Arrays
            .stream(importMetaData.propertyKeyIds())
            .anyMatch(keyId -> keyId != NO_SUCH_PROPERTY_KEY);

        var paging = pageSize.isPresent()
            ? new PagingWithKnownPageSize(pageSize.getAsInt())
            : new PagingWithUnknownPageSize(numPages);

        return new AdjacencyBuffer(
            importMetaData,
            adjacencyCompressorFactory,
            localBuilders,
            compressedAdjacencyLists,
            paging,
            atLeastOnePropertyToLoad
        );
    }

    private AdjacencyBuffer(
        SingleTypeRelationshipImporter.ImportMetaData importMetaData,
        AdjacencyCompressorFactory adjacencyCompressorFactory,
        ThreadLocalRelationshipsBuilder[] localBuilders,
        ChunkedAdjacencyLists[] chunkedAdjacencyLists,
        AdjacencyBufferPaging paging,
        boolean atLeastOnePropertyToLoad
    ) {
        this.adjacencyCompressorFactory = adjacencyCompressorFactory;
        this.localBuilders = localBuilders;
        this.chunkedAdjacencyLists = chunkedAdjacencyLists;
        this.paging = paging;
        this.relationshipCounter = adjacencyCompressorFactory.relationshipCounter();
        this.propertyKeyIds = importMetaData.propertyKeyIds();
        this.defaultValues = importMetaData.defaultValues();
        this.aggregations = importMetaData.aggregations();
        this.atLeastOnePropertyToLoad = atLeastOnePropertyToLoad;
    }

    /**
     * @param batch          two-tuple values sorted by source (source, target)
     * @param targets        slice of batch on second position; all targets in source-sorted order
     * @param propertyValues index-synchronised with targets. the list for each index are the properties for that source-target combo. null if no props
     * @param offsets        offsets into targets; every offset position indicates a source node group
     * @param length         length of offsets array (how many source tuples to import)
     */
    void addAll(
        long[] batch,
        long[] targets,
        @Nullable long[][] propertyValues,
        int[] offsets,
        int length
    ) {
        var paging = this.paging;

        ThreadLocalRelationshipsBuilder builder = null;
        int lastPageIndex = -1;
        int endOffset, startOffset = 0;
        try {
            for (int i = 0; i < length; ++i) {
                endOffset = offsets[i];

                // if there are no rels for this node, just go to next
                if (endOffset <= startOffset) {
                    continue;
                }

                long source = batch[startOffset << 1];

                int pageIndex = paging.pageId(source);

                if (pageIndex != lastPageIndex) {
                    // switch to the builder for this page
                    if (builder != null) {
                        builder.unlock();
                    }
                    builder = localBuilders[pageIndex];
                    builder.lock();
                    lastPageIndex = pageIndex;
                }

                long localId = paging.localId(source);

                ChunkedAdjacencyLists compressedTargets = this.chunkedAdjacencyLists[pageIndex];

                var targetsToImport = endOffset - startOffset;
                if (propertyValues == null) {
                    compressedTargets.add(localId, targets, startOffset, endOffset, targetsToImport);
                } else {
                    if (aggregations[0] != Aggregation.NONE && targetsToImport > 1) {
                        targetsToImport = preAggregate(targets, propertyValues, startOffset, endOffset, aggregations);
                    }
                    compressedTargets.add(localId, targets, propertyValues, startOffset, endOffset, targetsToImport);
                }

                startOffset = endOffset;
            }
        } finally {
            if (builder != null && builder.isLockedByCurrentThread()) {
                builder.unlock();
            }
        }
    }

    Collection<AdjacencyListBuilderTask> adjacencyListBuilderTasks(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        adjacencyCompressorFactory.init();

        var tasks = new ArrayList<AdjacencyListBuilderTask>(localBuilders.length + 1);
        for (int page = 0; page < localBuilders.length; page++) {
            tasks.add(new AdjacencyListBuilderTask(
                page,
                paging,
                localBuilders[page],
                chunkedAdjacencyLists[page],
                relationshipCounter,
                mapper.orElse(ZigZagLongDecoding.Identity.INSTANCE),
                drainCountConsumer.orElse(n -> {})
            ));
        }

        return tasks;
    }

    int[] getPropertyKeyIds() {
        return propertyKeyIds;
    }

    double[] getDefaultValues() {
        return defaultValues;
    }

    Aggregation[] getAggregations() {
        return aggregations;
    }

    boolean atLeastOnePropertyToLoad() {
        return atLeastOnePropertyToLoad;
    }

    /**
     * Responsible for writing a page of ChunkedAdjacencyLists into the adjacency list.
     */
    public static final class AdjacencyListBuilderTask implements Runnable {

        private final int page;
        private final AdjacencyBufferPaging paging;
        private final ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder;
        private final ChunkedAdjacencyLists chunkedAdjacencyLists;
        // A long array that may or may not be used during the compression.
        private final LongArrayBuffer buffer;
        private final LongAdder relationshipCounter;
        private final AdjacencyCompressor.ValueMapper valueMapper;
        private final LongConsumer drainCountConsumer;

        AdjacencyListBuilderTask(
            int page,
            AdjacencyBufferPaging paging,
            ThreadLocalRelationshipsBuilder threadLocalRelationshipsBuilder,
            ChunkedAdjacencyLists chunkedAdjacencyLists,
            LongAdder relationshipCounter,
            AdjacencyCompressor.ValueMapper valueMapper,
            LongConsumer drainCountConsumer
        ) {
            this.page = page;
            this.paging = paging;
            this.threadLocalRelationshipsBuilder = threadLocalRelationshipsBuilder;
            this.chunkedAdjacencyLists = chunkedAdjacencyLists;
            this.valueMapper = valueMapper;
            this.drainCountConsumer = drainCountConsumer;
            this.buffer = new LongArrayBuffer();
            this.relationshipCounter = relationshipCounter;
        }

        @Override
        public void run() {
            try (var compressor = threadLocalRelationshipsBuilder.intoCompressor()) {
                var importedRelationships = new MutableLong(0L);
                chunkedAdjacencyLists.consume((localId, targets, properties, compressedByteSize, numberOfCompressedTargets) -> {
                    var sourceNodeId = this.paging.sourceNodeId(localId, this.page);
                    var nodeId = valueMapper.map(sourceNodeId);
                    importedRelationships.add(compressor.applyVariableDeltaEncoding(
                        nodeId,
                        targets,
                        properties,
                        numberOfCompressedTargets,
                        compressedByteSize,
                        buffer,
                        valueMapper
                    ));
                });
                relationshipCounter.add(importedRelationships.longValue());
                drainCountConsumer.accept(importedRelationships.longValue());
            }
        }
    }

    private static final class PagingWithKnownPageSize implements AdjacencyBufferPaging {
        private final int pageShift;
        private final int pageMask;

        PagingWithKnownPageSize(int pageSize) {
            this.pageShift = Integer.numberOfTrailingZeros(pageSize);
            this.pageMask = pageSize - 1;
        }

        @Override
        public int pageId(long source) {
            return (int) (source >>> this.pageShift);
        }

        @Override
        public long localId(long source) {
            return (source & this.pageMask);
        }

        @Override
        public long sourceNodeId(long localId, int pageId) {
            return (((long) pageId) << this.pageShift) + localId;
        }
    }

    private static final class PagingWithUnknownPageSize implements AdjacencyBufferPaging {
        private final int pageShift;
        private final int pageMask;

        PagingWithUnknownPageSize(int numberOfPages) {
            this.pageShift = Integer.numberOfTrailingZeros(numberOfPages);
            this.pageMask = numberOfPages - 1;
        }

        @Override
        public int pageId(long source) {
            // TODO: we could not use the least-significant-bits to get the pageIndex
            //  instead we shift the source by, say, 8 bits and apply the mask on those bits
            //  this will give us some grouping within the lowest bits
            return (int) (source & this.pageMask);
        }

        @Override
        public long localId(long source) {
            return (source >>> this.pageShift);
        }

        @Override
        public long sourceNodeId(long localId, int pageId) {
            return (localId << this.pageShift) + pageId;
        }
    }
}
