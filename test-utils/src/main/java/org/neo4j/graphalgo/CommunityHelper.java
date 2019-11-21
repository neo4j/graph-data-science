/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphalgo.core.utils.ArrayUtil;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public final class CommunityHelper {

    public static void assertCommunities(HugeLongArray communityData, long[]... communities) {
        assertCommunities(communityData.toArray(), communities);
    }

    /**
     * Helper method that checks if the result of a community algorithm has the expected communities.
     * It only tests if members are in the same or different communities, given the input and
     * disregards specific community values.
     *
     * @param communityData The output of a community detection algorithm.
     * @param communities   The expected membership of communities. Elements within an array are
     *                      expected to be in the same community, whereas all elements of different
     *                      arrays are expected to be in different communities.
     */
    public static void assertCommunities(long[] communityData, long[]... communities) {
        for (long[] community : communities) {
            assertSameCommunity(communityData, community);
        }

        for (int i = 0; i < communities.length; i++) {
            for (int j = i + 1; j < communities.length; j++) {
                assertNotEquals(
                    communityData[(int) communities[i][0]], communityData[(int) communities[j][0]],
                    String.format(
                        "Expected node %d to be in a different community than node %d",
                        communities[i][0],
                        communities[j][0]
                    )
                );
            }
        }
    }

    public static void assertSameCommunity(HugeLongArray communities, long[] members) {
        assertSameCommunity(communities.toArray(), members);
    }

    public static void assertSameCommunity(long[] communities, long[] members) {
        long expectedCommunity = communities[(int) members[0]];

        for (int i = 1; i < members.length; i++) {
            long actualCommunity = communities[(int) members[i]];
            assertEquals(
                expectedCommunity,
                actualCommunity,
                String.format(
                    "Expected node %d (community %d) to have the same community as node %d (community %d)",
                    members[i],
                    actualCommunity,
                    members[0],
                    expectedCommunity
                )
            );
        }
    }

    public static void assertCommunitiesWithLabels(HugeLongArray communityData, Map<Long, Long[]> expectedCommunities) {
        assertCommunitiesWithLabels(communityData.toArray(), expectedCommunities);
    }

    public static void assertCommunitiesWithLabels(long[] communityData, Map<Long, Long[]> expectedCommunities) {
        long[][] longCommunities = new long[expectedCommunities.size()][];
        int i = 0;
        for (Long[] community : expectedCommunities.values()) {
           longCommunities[i++] = ArrayUtils.toPrimitive(community);
        }

        assertCommunities(communityData, longCommunities);
        for (Long label : expectedCommunities.keySet()) {
            Long[] community = expectedCommunities.get(label);
            for (Long nodeId : community) {
                assertEquals(label, communityData[nodeId.intValue()],
                    String.format(
                        "Expected node %d to be in community %d, but was %d",
                        nodeId,
                        label,
                        communityData[nodeId.intValue()]
                    )
                );
            }
        }
    }
}
