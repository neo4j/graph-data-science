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
package org.neo4j.graphalgo.core.utils.statistics;

import org.HdrHistogram.Histogram;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;

import java.util.function.LongUnaryOperator;

public final class CommunityStatistics {

    public static HugeSparseLongArray communitySizes(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        var componentSizeBuilder = HugeSparseLongArray.GrowingBuilder.create(0L, tracker);

        for (long nodeId = 0L; nodeId < nodeCount; nodeId++) {
            componentSizeBuilder.addTo(communityFunction.applyAsLong(nodeId), 1L);
        }
        return componentSizeBuilder.build();
    }

    public static long communityCount(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        return communityCount(communitySizes(nodeCount, communityFunction, tracker));
    }

    public static long communityCount(HugeSparseLongArray communitySizes) {
        long communityCount = 0L;

        for (long communityId = 0; communityId < communitySizes.getCapacity(); communityId++) {
            long communitySize = communitySizes.get(communityId);
            if (communitySize > 0) {
                communityCount++;
            }
        }

        return communityCount;
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(long nodeCount, LongUnaryOperator communityFunction, AllocationTracker tracker) {
        return communityCountAndHistogram(communitySizes(nodeCount, communityFunction, tracker));
    }

    public static CommunityCountAndHistogram communityCountAndHistogram(HugeSparseLongArray communitySizes) {
        var histogram = new Histogram(5);
        long communityCount = 0;
        for (long communityId = 0; communityId < communitySizes.getCapacity(); communityId++) {
            long communitySize = communitySizes.get(communityId);
            if (communitySize > 0) {
                communityCount++;
                histogram.recordValue(communitySize);
            }
        }

        return ImmutableCommunityCountAndHistogram.builder()
            .componentCount(communityCount)
            .histogram(histogram)
            .build();
    }

    private CommunityStatistics() {}

    @ValueClass
    public interface CommunityCountAndHistogram {
        long componentCount();

        Histogram histogram();
    }

}
