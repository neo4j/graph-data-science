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

import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.MemoryUsage;

public final class HugeSparseLongArrayUtil {

    private static final int PAGE_SHIFT = 12;
    private static final int PAGE_SIZE = 1 << PAGE_SHIFT;
    private static final int PAGE_MASK = PAGE_SIZE - 1;
    private static final long PAGE_SIZE_IN_BYTES = MemoryUsage.sizeOfLongArray(PAGE_SIZE);

    private HugeSparseLongArrayUtil() {}

    /**
     * @param maxId highest id that we need to represent
     * @param maxEntries number of identifiers we need to store
     */
    public static MemoryRange memoryEstimation(long maxId, long maxEntries) {
        assert(maxEntries <= maxId);
        int numPagesForSize = PageUtil.numPagesFor(maxId, PAGE_SHIFT, PAGE_MASK);
        int numPagesForMaxEntriesBestCase = PageUtil.numPagesFor(maxEntries, PAGE_SHIFT, PAGE_MASK);

        // Worst-case distribution assumes at least one entry per page
        final long maxEntriesForWorstCase = Math.min(maxId, maxEntries * PAGE_SIZE);
        int numPagesForMaxEntriesWorstCase = PageUtil.numPagesFor(maxEntriesForWorstCase, PAGE_SHIFT, PAGE_MASK);

        long classSize = MemoryUsage.sizeOfInstance(HugeSparseLongArray.class);
        long pagesSize = MemoryUsage.sizeOfObjectArray(numPagesForSize);
        long minRequirements = numPagesForMaxEntriesBestCase * PAGE_SIZE_IN_BYTES;
        long maxRequirements = numPagesForMaxEntriesWorstCase * PAGE_SIZE_IN_BYTES;
        return MemoryRange.of(classSize + pagesSize).add(MemoryRange.of(minRequirements, maxRequirements));
    }
}
