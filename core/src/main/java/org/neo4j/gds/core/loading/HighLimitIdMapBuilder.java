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

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;
import org.neo4j.gds.utils.CloseableThreadLocal;

public final class HighLimitIdMapBuilder implements IdMapBuilder {

    public static final String NAME = "highlimit";
    public static final byte ID = 3;

    private final ShardedLongLongMap.Builder intermediateIdMapBuilder;
    private final IdMapBuilder internalIdMapBuilder;

    private final CloseableThreadLocal<BulkAdder> adders;

    public static HighLimitIdMapBuilder of(int concurrency, IdMapBuilder internalIdMapBuilder) {
        return new HighLimitIdMapBuilder(concurrency, internalIdMapBuilder);
    }

    private HighLimitIdMapBuilder(int concurrency, IdMapBuilder internalIdMapBuilder) {
        // TODO: we might want to use the batched builder
        this.intermediateIdMapBuilder = ShardedLongLongMap.builder(concurrency);
        this.internalIdMapBuilder = internalIdMapBuilder;
        this.adders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public IdMapAllocator allocate(int batchLength) {
        var internalAllocator = this.internalIdMapBuilder.allocate(batchLength);
        var allocator = this.adders.get();
        allocator.reset(batchLength, internalAllocator);
        return allocator;
    }

    @Override
    public IdMap build(LabelInformation.Builder labelInformationBuilder, long highestNodeId, int concurrency) {
        var intermediateIdMap = this.intermediateIdMapBuilder.build();
        var internalIdMap = this.internalIdMapBuilder.build(labelInformationBuilder, highestNodeId, concurrency);

        return new HighLimitIdMap(intermediateIdMap, internalIdMap);
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder(intermediateIdMapBuilder);
    }

    private static final class BulkAdder implements IdMapAllocator {

        private final ShardedLongLongMap.Builder intermediateIdMapBuilder;
        private IdMapAllocator internalAllocator;
        private int batchLength = 0;

        private BulkAdder(ShardedLongLongMap.Builder intermediateIdMapBuilder) {
            this.intermediateIdMapBuilder = intermediateIdMapBuilder;
        }

        private void reset(int batchLength, IdMapAllocator internalAllocator) {
            this.batchLength = batchLength;
            this.internalAllocator = internalAllocator;
        }

        @Override
        public int allocatedSize() {
            return this.batchLength;
        }

        @Override
        public void insert(long[] nodeIds) {
            int length = this.batchLength;
            var intermediateIdMapBuilder = this.intermediateIdMapBuilder;

            // Replace the original nodeIds with the intermediate ones
            // in the input buffer as this one is reused by the caller
            // to insert node labels and property values.
            for (int i = 0; i < length; i++) {
                nodeIds[i] = intermediateIdMapBuilder.addNode(nodeIds[i]);
            }

            // Use intermediate ids for the internal id map builder.
            internalAllocator.insert(nodeIds);
        }
    }
}
