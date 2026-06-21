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
import org.neo4j.gds.api.nodes.FilterableNodeTranslator;
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.core.IdMapBehaviorServiceProvider;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ShardedLongLongMap;

/**
 * A {@link NodeTranslator} backed directly by a {@link ShardedLongLongMap}. The dense
 * mapped id space it produces is the same space its labels and properties are keyed by,
 * so it is exposed as the node translator of a {@link org.neo4j.gds.api.nodes.ComposedIdMap}.
 */
public final class ShardedIdMap implements FilterableNodeTranslator {

    private final ShardedLongLongMap idMap;

    ShardedIdMap(ShardedLongLongMap idMap) {
        this.idMap = idMap;
    }

    @Override
    public String typeId() {
        return ShardedIdMapBuilder.ID;
    }

    @Override
    public long nodeCount() {
        return idMap.size();
    }

    @Override
    public long highestOriginalId() {
        return idMap.maxOriginalId();
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return idMap.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return idMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public boolean containsOriginalId(long originalNodeId) {
        return idMap.contains(originalNodeId);
    }

    @Override
    public NodeTranslator filteredNodeTranslator(BitSet unionBitSet, Concurrency concurrency) {
        // The filtered representation (ArrayIdMap on community, BitIdMap on enterprise) is
        // chosen by the IdMapBehavior. ShardedIdMap lives in public/core and must NOT build a
        // BitIdMap directly, so it delegates the construction to the behavior.
        return IdMapBehaviorServiceProvider.idMapBehavior()
            .filteredNodeTranslator(unionBitSet, nodeCount(), concurrency);
    }
}
