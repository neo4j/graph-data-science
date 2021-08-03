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
package org.neo4j.gds.ml.core.subgraph;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryUsage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class LocalIdMap {
    private final LongArrayList originalIds;
    private final LongIntHashMap toInternalId;

    public static MemoryEstimation memoryEstimation(int numberOfClasses) {
        return MemoryEstimations.builder(LocalIdMap.class)
            .fixed("original IDs", MemoryUsage.sizeOfLongArray(numberOfClasses))
            .fixed("id mapping", MemoryUsage.sizeOfLongArray(numberOfClasses) + MemoryUsage.sizeOfIntArray(numberOfClasses))
            .build();
    }

    public LocalIdMap() {
        this.originalIds = new LongArrayList();
        this.toInternalId = new LongIntHashMap();
    }

    public int toMapped(long originalId) {
        if (toInternalId.containsKey(originalId)) {
            return toInternalId.get(originalId);
        }
        toInternalId.put(originalId, toInternalId.size());
        originalIds.add(originalId);
        return toInternalId.get(originalId);
    }

    public long toOriginal(int internalId) {
        return originalIds.get(internalId);
    }

    public long[] originalIds() {
        return originalIds.toArray();
    }

    public List<Long> originalIdsList() {
        var list = new ArrayList<Long>();
        originalIds.forEach((Consumer<LongCursor>) longCursor -> list.add(longCursor.value));
        return list;
    }

    public int size() {
        assert originalIds.size() == toInternalId.size();
        return originalIds.size();
    }
}
