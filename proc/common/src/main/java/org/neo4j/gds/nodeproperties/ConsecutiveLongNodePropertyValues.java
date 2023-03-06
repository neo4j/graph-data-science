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
package org.neo4j.gds.nodeproperties;

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.mem.BitUtil;

public class ConsecutiveLongNodePropertyValues implements LongNodePropertyValues {

    private static final long MAPPING_SIZE_QUOTIENT = 10L;
    private static final long NO_VALUE = -1L;


    private final HugeLongArray communities;

    public ConsecutiveLongNodePropertyValues(
        LongNodePropertyValues longNodeProperties,
        long nodeCount
    ) {
        var nextConsecutiveId = -1L;

        var setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
            nodeCount,
            MAPPING_SIZE_QUOTIENT
        ));

        this.communities = HugeLongArray.newArray(nodeCount);

        for (var nodeId = 0; nodeId < nodeCount; nodeId++) {
            if (longNodeProperties.hasValue(nodeId)) {
                var setId = longNodeProperties.longValue(nodeId);
                var communityId = setIdToConsecutiveId.getOrDefault(setId, -1);
                if (communityId == -1) {
                    setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                    communityId = nextConsecutiveId;
                }
                communities.set(nodeId, communityId);
            } else {
                communities.set(nodeId, NO_VALUE);
            }
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

    @Override
    public boolean hasValue(long nodeId) {
        return communities.get(nodeId) != NO_VALUE;
    }
}
