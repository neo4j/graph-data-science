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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.ToMappedNodeId;
import org.neo4j.gds.api.nodes.ComposedIdMap;
import org.neo4j.gds.api.nodes.FilterableNodeTranslator;
import org.neo4j.gds.api.nodes.LabelInformation;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.concurrent.atomic.LongAdder;

/**
 * A {@link NodeTranslator} whose mapped id space equals its original id space
 * ({@code toMappedNodeId(x) == x}). Useful when nodes are already keyed by a dense
 * id space and no remapping is required. Exposed as the node translator of a
 * {@link ComposedIdMap}.
 */
public final class IdentityIdMap implements FilterableNodeTranslator {

    private final long nodeCount;

    public IdentityIdMap(long nodeCount) {
        this.nodeCount = nodeCount;
    }

    @Override
    public String typeId() {
        return Builder.ID;
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public boolean containsOriginalId(long originalNodeId) {
        return originalNodeId < nodeCount();
    }

    @Override
    public long nodeCount() {
        return this.nodeCount;
    }

    @Override
    public long highestOriginalId() {
        return this.nodeCount - 1;
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return originalNodeId;
    }

    @Override
    public NodeTranslator filteredNodeTranslator(BitSet unionBitSet, Concurrency concurrency) {
        // The filtered representation (ArrayIdMap on community, BitIdMap on enterprise) is
        // chosen by the IdMapBehavior, so it is not built directly here.
        return IdMapBehaviorServiceProvider.idMapBehavior()
            .filteredNodeTranslator(unionBitSet, nodeCount(), concurrency);
    }

    public static class Builder implements IdMapBuilder {

        public static final String ID = "identity";

        private final LongAdder nodeCount;

        public Builder() {
            this.nodeCount = new LongAdder();
        }

        @Override
        public IdMapAllocator allocate(int batchLength) {
            return new IdMapAllocator() {
                @Override
                public int allocatedSize() {
                    return batchLength;
                }

                @Override
                public void insert(long[] nodeIds) {
                    nodeCount.add(batchLength);
                }
            };
        }

        @Override
        public ComposedIdMap build(
            LabelInformation.Builder labelInformationBuilder,
            long highestNodeId,
            Concurrency concurrency
        ) {
            var nodeCount = this.nodeCount.longValue();
            // The id map is identity, so label bit sets are already keyed by the mapped id and can
            // be built without remapping (see LabelInformation.Builder.IDENTITY).
            var labelInformation = labelInformationBuilder.build(nodeCount, ToMappedNodeId.IDENTITY);
            return ComposedIdMap.of(new IdentityIdMap(nodeCount), labelInformation);
        }
    }
}
