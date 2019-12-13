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

final class AbstractCommunityResultBuilderTest {
//
//    @Test
//    void countCommunitySizesOverHugeCommunities() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertTrue(maybeCommunityCount.isPresent());
//            assertTrue(maybeHistogram.isPresent());
//
//            long communityCount = maybeCommunityCount.orElse(-1);
//            Histogram histogram = maybeHistogram.get();
//
//            assertEquals(10L, communityCount, "should build 10 communities");
//            assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
//            assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
//        }, true, true);
//
//        builder
//            .withCommunityFunction(n -> n % 10L)
//            .withNodeCount(42)
//            .build();
//    }
//
//    @Test
//    void countCommunitySizesOverPresizedHugeCommunities() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertTrue(maybeCommunityCount.isPresent());
//            assertTrue(maybeHistogram.isPresent());
//
//            long communityCount = maybeCommunityCount.orElse(-1);
//            Histogram histogram = maybeHistogram.get();
//
//            assertEquals(10L, communityCount, "should build 10 communities");
//            assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
//            assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
//        }, true, true);
//
//        builder
//            .withExpectedNumberOfCommunities(10L)
//            .withCommunityFunction(n -> n % 10L)
//            .withNodeCount(42L)
//            .build();
//    }
//
//    @Test
//    void countCommunitySizesOverIntegerCommunities() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertTrue(maybeCommunityCount.isPresent());
//            assertTrue(maybeHistogram.isPresent());
//
//            long communityCount = maybeCommunityCount.orElse(-1);
//            Histogram histogram = maybeHistogram.get();
//
//            assertEquals(5L, communityCount, "should build 5 communities");
//            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
//            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
//        }, true, true);
//
//        builder
//            .withCommunityFunction(n -> (n / 10) + 1)
//            .withNodeCount(42)
//            .build();
//    }
//
//    @Test
//    void countCommunitySizesOverLongCommunities() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertTrue(maybeCommunityCount.isPresent());
//            assertTrue(maybeHistogram.isPresent());
//
//            long communityCount = maybeCommunityCount.orElse(-1);
//            Histogram histogram = maybeHistogram.get();
//
//            assertEquals(5L, communityCount, "should build 5 communities");
//            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
//            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
//        }, true, true);
//
//        builder
//            .withExpectedNumberOfCommunities(42)
//            .withCommunityFunction(n -> (int) (n / 10) + 1)
//            .withNodeCount(42)
//            .build();
//    }
//
//    @Test
//    void doNotGenerateCommunityCountOrHistogram() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertFalse(maybeCommunityCount.isPresent());
//            assertFalse(maybeHistogram.isPresent());
//        }, false, false);
//        builder
//            .withCommunityFunction(n -> n % 10L)
//            .withNodeCount(42L)
//            .build();
//    }
//
//    @Test
//    void doNotGenerateHistogram() {
//        AbstractCommunityResultBuilder<Void> builder = builder((maybeCommunityCount, maybeHistogram) -> {
//            assertTrue(maybeCommunityCount.isPresent());
//            assertFalse(maybeHistogram.isPresent());
//        }, false, true);
//
//        builder
//            .withCommunityFunction(n -> n % 10L)
//            .withNodeCount(42L)
//            .build();
//    }
//
//    @Test
//    void oneCommunityFromHugeMap() {
//        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
//        communitySizeMap.addTo(1, 4);
//
//        Histogram histogram = buildFrom(communitySizeMap);
//
//        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
//    }
//
//    @Test
//    void multipleCommunitiesFromHugeMap() {
//        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
//        communitySizeMap.addTo(1, 4);
//        communitySizeMap.addTo(2, 10);
//        communitySizeMap.addTo(3, 9);
//        communitySizeMap.addTo(4, 8);
//        communitySizeMap.addTo(5, 7);
//
//        Histogram histogram = buildFrom(communitySizeMap);
//
//        assertEquals(10.0, histogram.getValueAtPercentile(100D), 0.01);
//        assertEquals(8.0, histogram.getValueAtPercentile(50D), 0.01);
//    }
//
//    private Histogram buildFrom(Iterable<LongLongCursor> communitySizeMap) {
//        final Histogram histogram = new Histogram(5);
//
//        for (LongLongCursor cursor : communitySizeMap) {
//            histogram.recordValue(cursor.value);
//        }
//
//        return histogram;
//    }
//
//
//    private AbstractCommunityResultBuilder<Void> builder(
//        BiConsumer<OptionalLong, Optional<Histogram>> check,
//        boolean buildHistogram,
//        boolean buildCommunityCount
//    ) {
//        return new AbstractCommunityResultBuilder<Void>(buildHistogram, buildCommunityCount, AllocationTracker.EMPTY) {
//            @Override
//            protected Void buildResult() {
//                check.accept(maybeCommunityCount, maybeCommunityHistogram);
//                return null;
//            }
//        };
//    }
}
