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
package org.neo4j.graphalgo.nodeproperties;

import org.neo4j.graphalgo.api.nodeproperties.LongNodeProperties;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

public class ConsecutiveLongNodeProperties implements LongNodeProperties {

    private static final long MAPPING_SIZE_QUOTIENT = 10L;

    private final HugeLongArray communities;

    public ConsecutiveLongNodeProperties(
        LongNodeProperties longNodeProperties,
        long nodeCount,
        AllocationTracker tracker
    ) {
        var nextConsecutiveId = -1L;

        var setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
            nodeCount,
            MAPPING_SIZE_QUOTIENT
        ), tracker);

        this.communities = HugeLongArray.newArray(nodeCount, tracker);

        for (var nodeId = 0; nodeId < nodeCount; nodeId++) {
            var setId = longNodeProperties.longValue(nodeId);
            var communityId = setIdToConsecutiveId.getOrDefault(setId, -1);
            if (communityId == -1) {
                setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                communityId = nextConsecutiveId;
            }
            communities.set(nodeId, communityId);
        }
    }

    @Override
    public long longValue(long nodeId) {
        return communities.get(nodeId);
    }

    @Override
    public long size() {
        return communities.size();
    }
}
