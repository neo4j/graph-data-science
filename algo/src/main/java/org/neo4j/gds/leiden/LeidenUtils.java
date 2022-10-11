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
package org.neo4j.gds.leiden;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicLong;

class LeidenUtils {

    static HugeLongArray createSingleNodeCommunities(long nodeCount) {
        var array = HugeLongArray.newArray(nodeCount);
        array.setAll(v -> v);
        return array;
    }

    static HugeLongArray createSeedCommunities(long nodeCount, NodePropertyValues seedValues) {
        var array = HugeLongArray.newArray(nodeCount);
        long longMaxId = seedValues.getMaxLongPropertyValue().getAsLong();
        if (longMaxId < 0) {
            longMaxId = 0;
        }
        var maxId = new AtomicLong(longMaxId);
        array.setAll(v -> {
            long seedValue = seedValues.longValue(v);

            if (seedValue < 0 && seedValue != DefaultValue.LONG_DEFAULT_FALLBACK) {
                throw new IllegalArgumentException("Seed values should be non-negative");
            }
            return (seedValue >= 0) ? seedValue : maxId.incrementAndGet();
        });
        return array;
    }

    static HugeLongArray createStartingCommunities(long nodeCount, @Nullable NodePropertyValues seedValues) {
        return (seedValues == null) ? createSingleNodeCommunities(nodeCount) : createSeedCommunities(
            nodeCount,
            seedValues
        );
    }
}
