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

    public static final String ID = "highlimit";

    private final ShardedLongLongMap.BatchedBuilder originalToIntermediateMapping;
    private final IdMapBuilder intermediateToInternalMapping;

    private final CloseableThreadLocal<BulkAdder> bulkAdders;

    public static HighLimitIdMapBuilder of(int concurrency, IdMapBuilder internalIdMapBuilder) {
        return new HighLimitIdMapBuilder(concurrency, internalIdMapBuilder);
    }

    private HighLimitIdMapBuilder(int concurrency, IdMapBuilder internalIdMapBuilder) {
        // We use a builder that overrides the node ids in the input batch with the
        // generated intermediate node ids. This is necessary for downstream label
        // and property processing.
        this.originalToIntermediateMapping = ShardedLongLongMap.batchedBuilder(concurrency, true);
        this.intermediateToInternalMapping = internalIdMapBuilder;
        this.bulkAdders = CloseableThreadLocal.withInitial(this::newBulkAdder);
    }

    @Override
    public IdMapAllocator allocate(int batchLength) {
        var batch = this.originalToIntermediateMapping.prepareBatch(batchLength);
        var internalAllocator = this.intermediateToInternalMapping.allocate(batchLength);
        var bulkAdder = this.bulkAdders.get();
        bulkAdder.reset(batchLength, internalAllocator, batch);
        return bulkAdder;
    }

    @Override
    public IdMap build(LabelInformation.Builder labelInformationBuilder, long highestNodeId, int concurrency) {
        var intermediateIdMap = this.originalToIntermediateMapping.build();
        var internalIdMap = this.intermediateToInternalMapping.build(labelInformationBuilder, highestNodeId, concurrency);

        return new HighLimitIdMap(intermediateIdMap, internalIdMap);
    }

    private BulkAdder newBulkAdder() {
        return new BulkAdder();
    }

    private static final class BulkAdder implements IdMapAllocator {

        private IdMapAllocator intermediateAllocator;
        private IdMapAllocator internalAllocator;
        private int batchLength = 0;

        private void reset(int batchLength, IdMapAllocator internalAllocator, IdMapAllocator intermediateAllocator) {
            this.batchLength = batchLength;
            this.internalAllocator = internalAllocator;
            this.intermediateAllocator = intermediateAllocator;
        }

        @Override
        public int allocatedSize() {
            return this.batchLength;
        }

        @Override
        public void insert(long[] nodeIds) {
            this.intermediateAllocator.insert(nodeIds);
            this.internalAllocator.insert(nodeIds);
        }
    }
}
