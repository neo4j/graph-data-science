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
import org.neo4j.gds.api.nodes.NodeTranslator;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

/**
 * Builds the community-edition (array-backed) filtered {@link NodeTranslator} over a dense
 * root mapped id space. Lives in {@code core.loading} so it can reach the package-private
 * {@link ArrayIdMapBuilderOps}; the enterprise edition supplies a {@code BitIdMap}-backed
 * translator via its {@link org.neo4j.gds.core.IdMapBehavior} instead.
 */
public final class FilteredIdMapFactory {

    private FilteredIdMapFactory() {}

    public static NodeTranslator arrayBasedTranslator(
        BitSet unionBitSet,
        long rootNodeCount,
        Concurrency concurrency
    ) {
        long filteredNodeCount = unionBitSet.cardinality();
        HugeLongArray filteredToRoot = HugeLongArray.newArray(filteredNodeCount);

        long rootMappedId = -1L;
        long cursor = 0L;
        while ((rootMappedId = unionBitSet.nextSetBit(rootMappedId + 1)) != -1) {
            filteredToRoot.set(cursor++, rootMappedId);
        }

        // The root mapped space is dense 0..rootNodeCount-1.
        long highestRootMappedId = rootNodeCount - 1;

        HugeSparseLongArray rootToFiltered = ArrayIdMapBuilderOps.buildSparseIdMap(
            filteredNodeCount,
            highestRootMappedId,
            concurrency,
            filteredToRoot
        );

        return new ArrayIdMap(filteredToRoot, rootToFiltered, filteredNodeCount, highestRootMappedId);
    }
}
