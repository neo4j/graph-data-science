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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Collection;

public final class FilteredIdMapFactory {

    private FilteredIdMapFactory() {}

    /**
     * Builds an {@link ArrayIdMap}-backed filtered id map over the dense root mapped
     * space selected by {@code nodeLabels}. This is the community-edition representation.
     */
    public static FilteredIdMap arrayBased(
        IdMap rootIdMap,
        LabelInformation labelInformation,
        Collection<NodeLabel> nodeLabels,
        Concurrency concurrency
    ) {
        BitSet unionBitSet = labelInformation.unionBitSet(nodeLabels, rootIdMap.nodeCount());

        long nodeId = -1L;
        long cursor = 0L;
        long newNodeCount = unionBitSet.cardinality();
        HugeLongArray newGraphIds = HugeLongArray.newArray(newNodeCount);
        while ((nodeId = unionBitSet.nextSetBit(nodeId + 1)) != -1) {
            newGraphIds.set(cursor, nodeId);
            cursor++;
        }

        HugeSparseLongArray newNodeToGraphIds = ArrayIdMapBuilderOps.buildSparseIdMap(
            newNodeCount,
            rootIdMap.nodeCount() - 1,
            concurrency,
            newGraphIds
        );

        LabelInformation newLabelInformation = labelInformation.filter(nodeLabels);

        var rootToFilteredIdMap = new ArrayIdMap(
            newGraphIds,
            newNodeToGraphIds,
            newLabelInformation,
            newNodeCount,
            rootIdMap.nodeCount() - 1
        );

        return new FilteredLabeledIdMap(rootIdMap, rootToFilteredIdMap);
    }
}
