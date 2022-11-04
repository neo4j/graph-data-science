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

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

interface SeedCommunityManager {

    long mapToSeed(long communityId);

    static SeedCommunityManager create(boolean isSeeded, HugeLongArray startingNodeCommunities) {
        if (!isSeeded) {
            return new EmptySeedCommunityManager();
        } else {
            return FullSeedCommunityManager.create(startingNodeCommunities);
        }
    }

    class EmptySeedCommunityManager implements SeedCommunityManager {
        EmptySeedCommunityManager() {}

        @Override
        public long mapToSeed(long communityId) {
            return communityId;
        }
    }

    class FullSeedCommunityManager implements SeedCommunityManager {

        private HugeLongArray reverseMap;

        FullSeedCommunityManager(HugeLongArray reverseMap) {
            this.reverseMap = reverseMap;
        }

        static FullSeedCommunityManager create(HugeLongArray startingCommunities) {
            long nodeCount = startingCommunities.size();
            LongLongMap seedMap = new LongLongHashMap();
            long maxId = 0;
            for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
                long communityId = startingCommunities.get(nodeId);
                if (!seedMap.containsKey(communityId)) {
                    maxId = nodeId;
                    seedMap.put(communityId, nodeId);
                }
                startingCommunities.set(nodeId, seedMap.get(communityId));
            }
            HugeLongArray reverseMap = HugeLongArray.newArray(maxId + 1);
            for (LongLongCursor cursor : seedMap) {
                reverseMap.set(cursor.value, cursor.key);
            }
            return new FullSeedCommunityManager(reverseMap);
        }

        @Override
        public long mapToSeed(long communityId) {
            return reverseMap.get(communityId);
        }

    }
}
