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
package org.neo4j.gds.embeddings.node2vec;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PositiveSampleProducerTest {

    private final long[] buffer = new long[2];
    private final HugeDoubleArray centerNodeProbabilities = HugeDoubleArray.of(
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0
    );

    @Test
    void doesNotCauseStackOverflow() {
        // enough walks to potentially trigger StackOverflow
        int nbrOfWalks = 5000;

        var walks = createCompressedRandomWalks(nbrOfWalks, (l1) -> new long[]{l1});

        // our sample producer is supposed to work within the first 3 walks
        // it prefetches in the constructor
        var sampleProducer = new PositiveSampleProducer(
            walks.iterator(0, nbrOfWalks),
            HugeDoubleArray.of(LongStream.range(0, nbrOfWalks).mapToDouble((l) -> 1.0).toArray()),
            10,
            TestProgressLogger.NULL_LOGGER
        );

        var counter = 0L;
        while (sampleProducer.next(new long[2])) {
            counter++;
        }

        // does not overflow the stack = passes test
        assertEquals(0, counter, "no samples possible here because walks are too short");
    }

    @Test
    void doesNotCauseStackOverflowDueToBadLuck() {
        // enough walks to potentially trigger StackOverflow
        int nbrOfWalks = 5000;
        var walks = createCompressedRandomWalks(nbrOfWalks, (l1) -> new long[]{l1, (l1 + 1) % nbrOfWalks});

        // our sample producer is supposed to work within the first nbrOfWalks - 1 walks
        // it prefetches in the constructor
        HugeDoubleArray probabilities = HugeDoubleArray.of(LongStream
            .range(0, nbrOfWalks)
            .mapToDouble((l) -> 0)
            .toArray());
        var sampleProducer = new PositiveSampleProducer(
            walks.iterator(0, nbrOfWalks),
            probabilities,
            10,
            TestProgressLogger.NULL_LOGGER
        );
        // does not overflow the stack = passes test

        var counter = 0L;
        while (sampleProducer.next(new long[2])) {
            counter++;
        }

        assertEquals(0, counter, "no samples possible here because walks are too short");
    }

    @Test
    void doesNotAttemptToFetchOutsideBatch() {
        int nbrOfWalks = 100;
        var walks = createCompressedRandomWalks(
            nbrOfWalks,
            (l1) -> new long[]{l1, (l1 + 1) % nbrOfWalks, (l1 + 2) % nbrOfWalks}
        );

        var sampleProducer = new PositiveSampleProducer(
            walks.iterator(0, nbrOfWalks / 2),
            HugeDoubleArray.of(LongStream.range(0, nbrOfWalks).mapToDouble((l) -> 1.0).toArray()),
            10,
            TestProgressLogger.NULL_LOGGER
        );

        var counter = 0L;
        while (sampleProducer.next(new long[2])) {
            counter++;
        }

        assertEquals(300, counter, "50 walks in batch, 3 nodes each, 2 center word matches => 300 samples");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("org.neo4j.gds.embeddings.node2vec.PositiveSampleProducerTest#pairCombinations")
    void shouldProducePairsWith(
        String name,
        int windowSize,
        CompressedRandomWalks walks,
        List<Pair<Long, Long>> expectedPairs
    ) {
        Collection<Pair<Long, Long>> actualPairs = new ArrayList<>();

        PositiveSampleProducer producer = new PositiveSampleProducer(
            walks.iterator(0, walks.size()),
            centerNodeProbabilities,
            windowSize,
            TestProgressLogger.NULL_LOGGER
        );
        while (producer.next(buffer)) {
            actualPairs.add(Pair.of(buffer[0], buffer[1]));
        }

        assertEquals(expectedPairs, actualPairs);
    }

    @Test
    void shouldProducePairsWithBounds() {
        var walks = createCompressedRandomWalks(
            new long[]{0, 1, 2},
            new long[]{3, 4, 5},
            new long[]{3, 4, 5},
            new long[]{3, 4, 5}
        );

        Collection<Pair<Long, Long>> actualPairs = new ArrayList<>();
        PositiveSampleProducer producer = new PositiveSampleProducer(
            walks.iterator(0, 2),
            centerNodeProbabilities,
            3,
            TestProgressLogger.NULL_LOGGER
        );
        while (producer.next(buffer)) {
            actualPairs.add(Pair.of(buffer[0], buffer[1]));
        }

        assertEquals(
            List.of(
                Pair.of(0L, 1L),
                Pair.of(1L, 0L),
                Pair.of(1L, 2L),
                Pair.of(2L, 1L),

                Pair.of(3L, 4L),
                Pair.of(4L, 3L),
                Pair.of(4L, 5L),
                Pair.of(5L, 4L)
            ),
            actualPairs
        );
    }

    @Test
    void shouldRemoveDownsampledWordFromWalk() {
        var walks = createCompressedRandomWalks(
            new long[]{0, 1},       // 1 is downsampled, and the walk is then too short and will be ignored
            new long[]{0, 1, 2},    // 1 is downsampled, the remaining walk is (0,2)
            new long[]{3, 4, 5, 6}, // 5 is downsampled, the remaining walk is (3,4,6)
            new long[]{3, 4, 5}     // 5 is downsampled, the remaining walk is (3,4)
        );

        HugeDoubleArray centerNodeProbabilities = HugeDoubleArray.of(
            1.0,
            0.0,
            1.0,
            1.0,
            1.0,
            0.0,
            1.0
        );

        Collection<Pair<Long, Long>> actualPairs = new ArrayList<>();
        PositiveSampleProducer producer = new PositiveSampleProducer(
            walks.iterator(0, walks.size()),
            centerNodeProbabilities,
            3,
            TestProgressLogger.NULL_LOGGER
        );

        while (producer.next(buffer)) {
            actualPairs.add(Pair.of(buffer[0], buffer[1]));
        }

        assertEquals(
            List.of(
                Pair.of(0L, 2L),
                Pair.of(2L, 0L),

                Pair.of(3L, 4L),
                Pair.of(4L, 3L),
                Pair.of(4L, 6L),
                Pair.of(6L, 4L),

                Pair.of(3L, 4L),
                Pair.of(4L, 3L)
            ),
            actualPairs
        );
    }

    static Stream<Arguments> pairCombinations() {
        return Stream.of(
            arguments(
                "Uneven window size",
                3,
                createCompressedRandomWalks(
                    new long[]{0, 1, 2}
                ),
                List.of(
                    Pair.of(0L, 1L),
                    Pair.of(1L, 0L),
                    Pair.of(1L, 2L),
                    Pair.of(2L, 1L)
                )
            ),

            arguments(
                "Even window size",
                4,
                createCompressedRandomWalks(
                    new long[]{0, 1, 2, 3}
                ),
                List.of(
                    Pair.of(0L, 1L),
                    Pair.of(1L, 0L),
                    Pair.of(1L, 2L),
                    Pair.of(2L, 0L),
                    Pair.of(2L, 1L),
                    Pair.of(2L, 3L),
                    Pair.of(3L, 1L),
                    Pair.of(3L, 2L)
                )
            ),

            arguments(
                "Window size greater than walk length",
                3,
                createCompressedRandomWalks(
                    new long[]{0, 1}
                ),
                List.of(
                    Pair.of(0L, 1L),
                    Pair.of(1L, 0L)
                )
            ),

            arguments(
                "Multiple walks",
                3,
                createCompressedRandomWalks(
                    new long[]{0, 1, 2},
                    new long[]{3, 4, 5}
                ),
                List.of(
                    Pair.of(0L, 1L),
                    Pair.of(1L, 0L),
                    Pair.of(1L, 2L),
                    Pair.of(2L, 1L),

                    Pair.of(3L, 4L),
                    Pair.of(4L, 3L),
                    Pair.of(4L, 5L),
                    Pair.of(5L, 4L)
                )
            ),

            arguments(
                "Walks with different lengths",
                3,
                createCompressedRandomWalks(
                    new long[]{0, 1},
                    new long[]{3, 4, 5}
                ),
                List.of(
                    Pair.of(0L, 1L),
                    Pair.of(1L, 0L),

                    Pair.of(3L, 4L),
                    Pair.of(4L, 3L),
                    Pair.of(4L, 5L),
                    Pair.of(5L, 4L)
                )
            )
        );
    }

    private static CompressedRandomWalks createCompressedRandomWalks(long[]... walksInput) {
        var walks = new CompressedRandomWalks(walksInput.length, AllocationTracker.empty());
        for (long[] walk : walksInput) {
            walks.add(walk);
        }
        return walks;
    }

    private static CompressedRandomWalks createCompressedRandomWalks(long count, WalkSupplier walkSupplier) {
        var walks = new CompressedRandomWalks(count, AllocationTracker.empty());
        for (long i = 0; i < count; i++) {
            walks.add(walkSupplier.getWalk(i));
        }
        return walks;
    }

    interface WalkSupplier {
        long[] getWalk(long index);
    }

}
