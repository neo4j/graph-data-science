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
package org.neo4j.gds.core.utils.statistics;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.statistics.CommunityStatistics;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.toArguments;

class CommunityStatisticsTest {

    @ParameterizedTest
    @MethodSource("testInput")
    void communitySizes(
        long nodeCount,
        LongUnaryOperator communityFunction,
        long expectedCommunityCount,
        Map<Long, Long> expectedCommunitySizes,
        Histogram expectedCommunitySizeHistogram,
        int concurrency
    ) {
        var communitySizes = CommunityStatistics.communitySizes(
            nodeCount,
            communityFunction,
            Pools.DEFAULT,
            concurrency,
            AllocationTracker.empty()
        );
        expectedCommunitySizes.forEach((communityId, expectedSize) -> {
            assertEquals(expectedSize, communitySizes.get(communityId));
        });
    }

    @ParameterizedTest
    @MethodSource("testInput")
    void communityCount(
        long nodeCount,
        LongUnaryOperator communityFunction,
        long expectedCommunityCount,
        Map<Long, Long> expectedCommunitySizes,
        Histogram expectedCommunitySizeHistogram,
        int concurrency
    ) {
        assertEquals(
            expectedCommunityCount,
            CommunityStatistics.communityCount(
                nodeCount,
                communityFunction,
                Pools.DEFAULT,
                concurrency,
                AllocationTracker.empty()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("testInput")
    void communityCountAndHistogram(
        long nodeCount,
        LongUnaryOperator communityFunction,
        long expectedCommunityCount,
        Map<Long, Long> expectedCommunitySizes,
        Histogram expectedCommunitySizeHistogram,
        int concurrency
    ) {
        var communityCountAndHistogram = CommunityStatistics.communityCountAndHistogram(
            nodeCount,
            communityFunction,
            Pools.DEFAULT,
            concurrency,
            AllocationTracker.empty()
        );

        assertEquals(expectedCommunityCount, communityCountAndHistogram.componentCount());
        assertEquals(expectedCommunitySizeHistogram, communityCountAndHistogram.histogram());
    }

    private static Stream<Arguments> testInput() {
        return TestSupport.crossArguments(
            CommunityStatisticsTest::expectedResults,
            toArguments(CommunityStatisticsTest::concurrencies)
        );
    }

    private static Stream<Integer> concurrencies() {
        return Stream.of(1, 4);
    }

    private static Stream<Arguments> expectedResults() {
        return Stream.of(
            Arguments.of(
                1000,
                LongUnaryOperator.identity(),
                1000,
                communitySizes(1000, LongUnaryOperator.identity()),
                communitySizeHistogram(communitySizes(1000, LongUnaryOperator.identity()))
            ),
            Arguments.of(
                1000,
                (LongUnaryOperator) operand -> operand % 2,
                2,
                communitySizes(1000, operand -> operand % 2),
                communitySizeHistogram(communitySizes(1000, operand -> operand % 2))
            ),
            Arguments.of(
                1000,
                (LongUnaryOperator) operand -> operand % 4,
                4,
                communitySizes(1000, operand -> operand % 4),
                communitySizeHistogram(communitySizes(1000, operand -> operand % 4))
            )
        );
    }

    private static Map<Long, Long> communitySizes(long nodeCount, LongUnaryOperator communityFunction) {
        var communitySizes = new HashMap<Long, Long>();
        for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
            communitySizes.compute(communityFunction.applyAsLong(nodeId), (k, v) -> v == null ? 1 : v + 1);
        }
        return communitySizes;
    }

    private static Histogram communitySizeHistogram(Map<Long, Long> communitySizes) {
        var histogram = new Histogram(5);
        communitySizes.forEach((communityId, communitySize) -> histogram.recordValue(communitySize));
        return histogram;
    }
}
