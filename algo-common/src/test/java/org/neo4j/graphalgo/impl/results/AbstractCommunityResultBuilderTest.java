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
package org.neo4j.graphalgo.impl.results;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AbstractCommunityResultBuilderTest {

    private static final Set<String> COMMUNITY_COUNT_FIELD = Collections.singleton("communityCount");
    private static final Set<String> SET_COUNT_FIELD = Collections.singleton("setCount");
    private static final Set<String> HISTOGRAM_FIELDS = new HashSet<>(Arrays.asList("p01", "p75", "p100"));
    private static final Set<String> ALL_FIELDS = new HashSet<String>() {{
        this.addAll(COMMUNITY_COUNT_FIELD);
        this.addAll(SET_COUNT_FIELD);
        this.addAll(HISTOGRAM_FIELDS);
    }};

    @Test
    void countCommunitySizesOverHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(10L, communityCount, "should build 10 communities");
            assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
            assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
        }, ALL_FIELDS);

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void countCommunitySizesOverPresizedHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(10L, communityCount, "should build 10 communities");
            assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
            assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
        }, ALL_FIELDS);

        builder
            .withExpectedNumberOfCommunities(10L)
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42L)
            .build();
    }

    @Test
    void countCommunitySizesOverIntegerCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(5L, communityCount, "should build 5 communities");
            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
        }, ALL_FIELDS);

        builder
            .withCommunityFunction(n -> (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @Test
    void countCommunitySizesOverLongCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(5L, communityCount, "should build 5 communities");
            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
        }, ALL_FIELDS);

        builder
            .withExpectedNumberOfCommunities(42)
            .withCommunityFunction(n -> (int) (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @Test
    void doNotGenerateCommunityCountOrHistogram() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertFalse(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, Collections.emptySet());
        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42L)
            .build();
    }

    @Test
    void doNotGenerateHistogram() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, COMMUNITY_COUNT_FIELD);

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42L)
            .build();

        builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, SET_COUNT_FIELD);

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42L)
            .build();
    }

    @Test
    void oneCommunityFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);

        Histogram histogram = buildFrom(communitySizeMap);

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }

    @Test
    void multipleCommunitiesFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);
        communitySizeMap.addTo(2, 10);
        communitySizeMap.addTo(3, 9);
        communitySizeMap.addTo(4, 8);
        communitySizeMap.addTo(5, 7);

        Histogram histogram = buildFrom(communitySizeMap);

        assertEquals(10.0, histogram.getValueAtPercentile(100D), 0.01);
        assertEquals(8.0, histogram.getValueAtPercentile(50D), 0.01);
    }

    private Histogram buildFrom(Iterable<LongLongCursor> communitySizeMap) {
        final Histogram histogram = new Histogram(5);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        return histogram;
    }


    private AbstractCommunityResultBuilder<Void> builder(
            BiConsumer<OptionalLong, Optional<Histogram>> check,
            Set<String> resultFields) {
        return new AbstractCommunityResultBuilder<Void>(resultFields, AllocationTracker.EMPTY) {
            @Override
            protected Void buildResult() {
                check.accept(maybeCommunityCount, maybeCommunityHistogram);
                return null;
            }
        };
    }
}
