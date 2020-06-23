/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.subgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalIdMapImpl implements LocalIdMap {
    private final List<Long> originalIds;
    private final Map<Long, Integer> toInternalId;

    public LocalIdMapImpl() {
        this.originalIds = new ArrayList<>();
        this.toInternalId = new HashMap<>();
    }

    @Override
    public int toMapped(long originalId) {
        if (toInternalId.containsKey(originalId)) {
            return toInternalId.get(originalId);
        }
        toInternalId.put(originalId, toInternalId.size());
        originalIds.add(originalId);
        return toInternalId.get(originalId);
    }

    @Override
    public long toOriginal(int internalId) {
        return originalIds.get(internalId);
    }

    @Override
    public Collection<Long> originalIds() {
        return originalIds;
    }
}
