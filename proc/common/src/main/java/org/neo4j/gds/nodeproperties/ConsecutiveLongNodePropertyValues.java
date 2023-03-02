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
    private static final long NO_VALUE = -1L;


    private final HugeLongArray communities;

    public ConsecutiveLongNodePropertyValues(LongNodePropertyValues inputProperties) {
        var nextConsecutiveId = -1L;
        long maxIdx = inputProperties.maxIndex();
        var setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
            maxIdx,
            MAPPING_SIZE_QUOTIENT
        ));

        this.communities = HugeLongArray.newArray(maxIdx);

        for (var nodeId = 0; nodeId < maxIdx; nodeId++) {
            if (longNodeProperties.hasValue(nodeId)) {
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

    @Override
    public long longValue(long nodeId) {
        return communities.get(nodeId);
    }

    @Override
    public long valuesStored() {
        return communities.size();
    }

    @Override
    public boolean hasValue(long nodeId) {
        return communities.get(nodeId) != NO_VALUE;
    }

    @Override
    public Value value(long nodeId) {
        if (hasValue(nodeId)) {
            return Values.longValue(communities.get(nodeId));
        }
        return null;

    }

    @Override
    public long maxIndex() {
        return communities.size();
    }
}
