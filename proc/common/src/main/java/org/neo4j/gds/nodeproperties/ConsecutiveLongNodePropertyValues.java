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
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class ConsecutiveLongNodePropertyValues implements LongNodePropertyValues {

    private static final long MAPPING_SIZE_QUOTIENT = 10L;

    private final HugeLongArray communities;

    public ConsecutiveLongNodePropertyValues(LongNodePropertyValues inputProperties) {
        var nextConsecutiveId = -1L;
        var nextSkippableId = -2L; // communities signaled with <0 are skipped and not written

        long maxIdx = inputProperties.nodeCount();
        var setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
            maxIdx,
            MAPPING_SIZE_QUOTIENT
        ));

        this.communities = HugeLongArray.newArray(maxIdx);

        for (var nodeId = 0; nodeId < maxIdx; nodeId++) {
            var setId = inputProperties.longValue(nodeId);
            var communityId = setIdToConsecutiveId.getOrDefault(setId, -1);
            if (communityId == -1) {
                //if this is null, it  means this community should not be written
                if (longNodeProperties.value(nodeId) != null) {
                    setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                    communityId = nextConsecutiveId;
                } else {
                    setIdToConsecutiveId.addTo(setId, --nextSkippableId); //assign it a negative id
                    communityId = nextSkippableId;
                }
            }
            communities.set(nodeId, communityId);
        }
    }

    @Override
    public long longValue(long nodeId) {
        return communities.get(nodeId);
    }

    @Override
    public Value value(long nodeId) {
        if (communities.get(nodeId) < 0) { //community is skipped, return null
            return null;
        } else {
            return Values.longValue(communities.get(nodeId));
        }
    }

    @Override
    public long nodeCount() {
        return communities.size();
    }
}
