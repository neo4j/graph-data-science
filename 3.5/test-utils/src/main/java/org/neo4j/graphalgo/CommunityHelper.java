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

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
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
     * @param actual   The output of a community detection algorithm.
     * @param expected The expected membership of communities. Elements within an array are
     *                 expected to be in the same community, whereas all elements of different
     *                 arrays are expected to be in different communities.
     */
    public static void assertCommunities(long[] actual, long[]... expected) {
        List<Long> actualList = Arrays.stream(actual).boxed().collect(toList());
        List<List<Long>> expectedList = Arrays.stream(expected).map(
            a -> Arrays.stream(a).boxed().collect(toList())
        ).collect(toList());

        assertCommunities(actualList, expectedList);
    }

    public static void assertCommunities(List<Long> actualCommunityData, List<List<Long>> expectedCommunities) {
        for (List<Long> community : expectedCommunities) {
            assertSameCommunity(actualCommunityData, community);
        }

        for (int i = 0; i < expectedCommunities.size(); i++) {
            for (int j = i + 1; j < expectedCommunities.size(); j++) {
                int expected = expectedCommunities.get(i).get(0).intValue();
                int actual = expectedCommunities.get(j).get(0).intValue();
                assertNotEquals(
                    actualCommunityData.get(expected),
                    actualCommunityData.get(actual),
                    String.format(
                        "Expected node %d to be in a different community than node %d",
                        expected,
                        actual
                    )
                );
            }
        }
    }

    public static void assertSameCommunity(HugeLongArray communities, long[] members) {
        assertSameCommunity(communities.toArray(), members);
    }

    public static void assertSameCommunity(long[] communities, long[] members) {
        List<Long> communitiesList = Arrays.stream(communities).boxed().collect(toList());
        List<Long> membersList = Arrays.stream(members).boxed().collect(toList());
        assertSameCommunity(communitiesList, membersList);
    }

    public static void assertSameCommunity(List<Long> communities, List<Long> members) {
        long expectedCommunity = communities.get(members.get(0).intValue());

        for (int i = 1; i < members.size(); i++) {
            Long member = members.get(i);
            long actualCommunity = communities.get(member.intValue());
            assertEquals(
                expectedCommunity,
                actualCommunity,
                String.format(
                    "Expected node %d (community %d) to have the same community as node %d (community %d)",
                    member,
                    actualCommunity,
                    members.get(0),
                    expectedCommunity
                )
            );
        }
    }

    public static void assertCommunitiesWithLabels(HugeLongArray communityData, Map<Long, Long[]> expectedCommunities) {
        assertCommunitiesWithLabels(communityData.toArray(), expectedCommunities);
    }

    public static void assertCommunitiesWithLabels(long[] communityData, Map<Long, Long[]> expectedCommunities) {
        List<Long> communityDataList = Arrays.stream(communityData).boxed().collect(toList());
        List<List<Long>> expectedCommunitiesList = expectedCommunities
            .values()
            .stream()
            .map(l -> Arrays.stream(l).collect(toList()))
            .collect(toList());

        assertCommunities(communityDataList, expectedCommunitiesList);
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
