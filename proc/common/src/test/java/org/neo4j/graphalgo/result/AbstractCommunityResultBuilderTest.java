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
package org.neo4j.graphalgo.result;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.wcc.ImmutableWccWriteConfig;
import org.neo4j.graphalgo.wcc.WccWriteConfig;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.procedureCallContext;

final class AbstractCommunityResultBuilderTest {

    private static final WccWriteConfig DEFAULT_CONFIG = ImmutableWccWriteConfig.builder()
        .writeProperty("communities")
        .build();

    @Test
    void countCommunitySizesOverHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext("communityCount", "communityDistribution"),
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertTrue(maybeHistogram.isPresent());

                long communityCount = maybeCommunityCount.orElse(-1);
                Histogram histogram = maybeHistogram.get();

                assertEquals(10L, communityCount, "should build 10 communities");
                assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
                assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
            }
        );

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void countCommunitySizesOverPresizedHugeCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext("communityCount", "communityDistribution"),
            (maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(10L, communityCount, "should build 10 communities");
            assertEquals(2L, histogram.getCountAtValue(5L), "should 2 communities with 5 members");
            assertEquals(8L, histogram.getCountAtValue(4L), "should build 8 communities with 4 members");
        });

        builder
            .withExpectedNumberOfCommunities(10L)
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void countCommunitySizesOverIntegerCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext("communityCount", "communityDistribution"),
            (maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(5L, communityCount, "should build 5 communities");
            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
        });

        builder
            .withCommunityFunction(n -> (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @Test
    void countCommunitySizesOverLongCommunities() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext("communityCount", "communityDistribution"),
            (maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertTrue(maybeHistogram.isPresent());

            long communityCount = maybeCommunityCount.orElse(-1);
            Histogram histogram = maybeHistogram.get();

            assertEquals(5L, communityCount, "should build 5 communities");
            assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
            assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
        });

        builder
            .withExpectedNumberOfCommunities(42)
            .withCommunityFunction(n -> (int) (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @Test
    void doNotGenerateCommunityCountOrHistogram() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext(),
            (maybeCommunityCount, maybeHistogram) -> {
            assertFalse(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        });
        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void doNotGenerateHistogram() {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureCallContext("communityCount"),
            (maybeCommunityCount, maybeHistogram) -> {
            assertTrue(maybeCommunityCount.isPresent());
            assertFalse(maybeHistogram.isPresent());
        });

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void oneCommunityFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap(AllocationTracker.EMPTY);
        communitySizeMap.addTo(1, 4);

        final Histogram histogram = new Histogram(5);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }


    private AbstractCommunityResultBuilder<Void> builder(
        ProcedureCallContext context,
        BiConsumer<OptionalLong, Optional<Histogram>> check
    ) {
        return new AbstractCommunityResultBuilder<Void>(
            context,
            AllocationTracker.EMPTY
        ) {
            @Override
            protected Void buildResult() {
                check.accept(maybeCommunityCount, maybeCommunityHistogram);
                return null;
            }
        };
    }
}
