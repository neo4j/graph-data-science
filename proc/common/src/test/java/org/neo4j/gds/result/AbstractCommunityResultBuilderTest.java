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
package org.neo4j.gds.result;

import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;

import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AbstractCommunityResultBuilderTest {

    @ParameterizedTest
    @MethodSource("concurrencies")
    void countCommunitySizesOverHugeCommunities(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount", "communityDistribution"),
            concurrency,
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

    @ParameterizedTest
    @MethodSource("concurrencies")
    void countCommunitySizesOverPresizedHugeCommunities(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount", "communityDistribution"),
            concurrency,
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

    @ParameterizedTest
    @MethodSource("concurrencies")
    void countCommunitySizesOverIntegerCommunities(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount", "communityDistribution"),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertTrue(maybeHistogram.isPresent());

                long communityCount = maybeCommunityCount.orElse(-1);
                Histogram histogram = maybeHistogram.get();

                assertEquals(5L, communityCount, "should build 5 communities");
                assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
                assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
            }
        );

        builder
            .withCommunityFunction(n -> (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @ParameterizedTest
    @MethodSource("concurrencies")
    void countCommunitySizesOverLongCommunities(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount", "communityDistribution"),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertTrue(maybeHistogram.isPresent());

                long communityCount = maybeCommunityCount.orElse(-1);
                Histogram histogram = maybeHistogram.get();

                assertEquals(5L, communityCount, "should build 5 communities");
                assertEquals(4L, histogram.getCountAtValue(10L), "should build 4 communities with 10 member");
                assertEquals(1L, histogram.getCountAtValue(2L), "should build 1 community with 2 members");
            }
        );

        builder
            .withCommunityFunction(n -> (int) (n / 10) + 1)
            .withNodeCount(42)
            .build();
    }

    @ParameterizedTest
    @MethodSource("concurrencies")
    void doNotGenerateCommunityCountOrHistogram(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns(),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertFalse(maybeCommunityCount.isPresent());
                assertFalse(maybeHistogram.isPresent());
            }
        );
        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @ParameterizedTest
    @MethodSource("concurrencies")
    void doNotGenerateHistogram(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount"),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertFalse(maybeHistogram.isPresent());
            }
        );

        builder
            .withCommunityFunction(n -> n % 10L)
            .withNodeCount(42)
            .build();
    }

    @Test
    void oneCommunityFromHugeMap() {
        HugeLongLongMap communitySizeMap = new HugeLongLongMap();
        communitySizeMap.addTo(1, 4);

        final Histogram histogram = new Histogram(5);

        for (LongLongCursor cursor : communitySizeMap) {
            histogram.recordValue(cursor.value);
        }

        assertEquals(4.0, histogram.getValueAtPercentile(100D), 0.01);
    }

    @ParameterizedTest
    @MethodSource("concurrencies")
    void buildCommunityCountWithHugeCommunityCount(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount"),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertFalse(maybeHistogram.isPresent());
                long communityCount = maybeCommunityCount.orElse(-1);
                assertEquals(2L, communityCount, "should build 2 communities");
            }
        );

        LongUnaryOperator communityFunction = n -> n % 2 == 0 ? 0 : ((long) Integer.MAX_VALUE) + 2;
        builder
            .withCommunityFunction(communityFunction)
            .withNodeCount(2)
            .build();
    }

    @ParameterizedTest
    @MethodSource("concurrencies")
    void buildCommunityHistogramWithHugeCommunityCount(int concurrency) {
        AbstractCommunityResultBuilder<Void> builder = builder(
            procedureReturnColumns("communityCount", "communityDistribution"),
            concurrency,
            (maybeCommunityCount, maybeHistogram) -> {
                assertTrue(maybeCommunityCount.isPresent());
                assertTrue(maybeHistogram.isPresent());

                long communityCount = maybeCommunityCount.orElse(-1);
                Histogram histogram = maybeHistogram.get();

                assertEquals(2L, communityCount, "should build 2 communities");
                assertEquals(2L, histogram.getCountAtValue(1L), "should build 2 communities with 1 member");
            }
        );

        LongUnaryOperator communityFunction = n -> n % 2 == 0 ? 0 : ((long) Integer.MAX_VALUE) + 2;
        builder
            .withCommunityFunction(communityFunction)
            .withNodeCount(2)
            .build();
    }

    static Stream<Arguments> concurrencies() {
        return Stream.of(
            Arguments.of(1),
            Arguments.of(4)
        );
    }

    static ProcedureReturnColumns procedureReturnColumns(String... returnColumns) {
        return new ProcedureReturnColumns() {

            @Override
            public boolean contains(String fieldName) {
                return Arrays.asList(returnColumns).contains(fieldName);
            }
        };
    }

    private AbstractCommunityResultBuilder<Void> builder(
        ProcedureReturnColumns returnColumns,
        int concurrency,
        BiConsumer<OptionalLong, Optional<Histogram>> check
    ) {
        return new AbstractCommunityResultBuilder<>(
            returnColumns,
            concurrency
        ) {
            @Override
            protected Void buildResult() {
                check.accept(maybeCommunityCount, maybeCommunityHistogram);
                return null;
            }
        };
    }
}
