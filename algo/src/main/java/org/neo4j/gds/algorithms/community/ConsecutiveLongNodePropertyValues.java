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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.api.properties.nodes.FilteredNodePropertyValuesMarker;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.mem.BitUtil;

public class ConsecutiveLongNodePropertyValues implements LongNodePropertyValues, FilteredNodePropertyValuesMarker {

    private static final long MAPPING_SIZE_QUOTIENT = 10L;
    private static final long NO_VALUE = -1L;


    private final HugeLongArray communities;

    public ConsecutiveLongNodePropertyValues(LongNodePropertyValues inputProperties) {
        var nextConsecutiveId = -1L;
        long maxIdx = inputProperties.nodeCount();
        var setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
            maxIdx,
            MAPPING_SIZE_QUOTIENT
        ));

        this.communities = HugeLongArray.newArray(maxIdx);

        for (var nodeId = 0; nodeId < maxIdx; nodeId++) {
            if (inputProperties.hasValue(nodeId)) {
                var setId = inputProperties.longValue(nodeId);
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

    /**
     * Returning Long.MIN_VALUE indicates that the value should not be written to Neo4j.
     * <p>
     * The filter is applied in the latest stage before writing to Neo4j.
     * Since the wrapped node properties may have additional logic in longValue(),
     * we need to check if they already filtered the value. Only in the case
     * where the wrapped properties pass on the value, we can apply a filter.
     */
    @Override
    public long longValue(long nodeId) {
        long l = communities.get(nodeId);
        if (l == NO_VALUE) {
            return Long.MIN_VALUE;
        }
        return l;
    }

    @Override
    public boolean hasValue(long nodeId) {
        return communities.get(nodeId) != NO_VALUE;
    }

    @Override
    public long nodeCount() {
        return communities.size();
    }
}
