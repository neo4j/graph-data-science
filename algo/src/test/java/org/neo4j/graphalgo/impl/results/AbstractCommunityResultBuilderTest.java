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

import org.HdrHistogram.Histogram;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public final class AbstractCommunityResultBuilderTest {

    private static final Set<String> COMMUNITY_COUNT_FIELD = Collections.singleton("communityCount");
    private static final Set<String> SET_COUNT_FIELD = Collections.singleton("setCount");
    private static final Set<String> HISTOGRAM_FIELDS = new HashSet<>(Arrays.asList("p01", "p75", "p100"));
    private static final Set<String> ALL_FIELDS = new HashSet<String>() {{
        this.addAll(COMMUNITY_COUNT_FIELD);
        this.addAll(SET_COUNT_FIELD);
        this.addAll(HISTOGRAM_FIELDS);
    }};

    @Test
    public void countCommunitySizesOverHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals("should build 10 communities", 10L, communityCount);
            assertEquals("should 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
            assertEquals("should build 8 communities with 4 members", 8L, histogram.getCountAtValue(4L));
        }, ALL_FIELDS);
        builder.build(AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void countCommunitySizesOverPresizedHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals("should build 10 communities", 10L, communityCount);
            assertEquals("should 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
            assertEquals("should build 8 communities with 4 members", 8L, histogram.getCountAtValue(4L));
        }, ALL_FIELDS);
        builder.build(10L, AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void countCommunitySizesOverIntegerCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals("should build 42 communities", 42L, communityCount);
            assertEquals("should build 10 communities with 1 member", 10L, histogram.getCountAtValue(1L));
            assertEquals("should build 10 communities with 2 members", 10L, histogram.getCountAtValue(2L));
            assertEquals("should build 10 communities with 3 members", 10L, histogram.getCountAtValue(3L));
            assertEquals("should build 10 communities with 4 members", 10L, histogram.getCountAtValue(4L));
            assertEquals("should build 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
        }, ALL_FIELDS);

        builder.buildfromKnownSizes(42, n -> (n / 10) + 1);
    }

    @Test
    public void countCommunitySizesOverLongCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals("should build 42 communities", 42L, communityCount);
            assertEquals("should build 10 communities with 1 member", 10L, histogram.getCountAtValue(1L));
            assertEquals("should build 10 communities with 2 members", 10L, histogram.getCountAtValue(2L));
            assertEquals("should build 10 communities with 3 members", 10L, histogram.getCountAtValue(3L));
            assertEquals("should build 10 communities with 4 members", 10L, histogram.getCountAtValue(4L));
            assertEquals("should build 2 communities with 5 members", 2L, histogram.getCountAtValue(5L));
        }, ALL_FIELDS);
        builder.buildfromKnownLongSizes(42, n -> ((int) n / 10) + 1);
    }

    @Test
    public void doNotGenerateCommunityCountOrHistorgram() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertFalse(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, Collections.emptySet());
        builder.build(AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void doNotGenerateHistogram() {
        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, COMMUNITY_COUNT_FIELD);
        builder.build(AllocationTracker.EMPTY, 42L, n -> n % 10L);

        builder = builder((maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        }, SET_COUNT_FIELD);
        builder.build(AllocationTracker.EMPTY, 42L, n -> n % 10L);
    }

    @Test
    public void oneCommunityFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);

        Histogram histogram = AbstractCommunityResultBuilder.buildFrom(communitySizeMap);

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }

    @Test
    public void multipleCommunitiesFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);
        communitySizeMap.addTo(2, 10);
        communitySizeMap.addTo(3, 9);
        communitySizeMap.addTo(4, 8);
        communitySizeMap.addTo(5, 7);

        Histogram histogram = AbstractCommunityResultBuilder.buildFrom(communitySizeMap);

        assertEquals(10.0, histogram.getValueAtPercentile(100D), 0.01);
        assertEquals(8.0, histogram.getValueAtPercentile(50D), 0.01);
    }

    private AbstractCommunityResultBuilder<Void> builder(
            BiConsumer<OptionalLong, Optional<Histogram>> check,
            Set<String> resultFields) {
        return new AbstractCommunityResultBuilder<Void>(resultFields) {
            @Override
            protected Void build(
                    long loadMillis,
                    long computeMillis,
                    long writeMillis,
                    long postProcessingMillis,
                    long nodeCount,
                    OptionalLong maybeCommunityCount,
                    Optional<Histogram> maybeCommunityHistogram,
                    boolean write) {
                check.accept(maybeCommunityCount, maybeCommunityHistogram);
                return null;
            }
        };
    }
}
