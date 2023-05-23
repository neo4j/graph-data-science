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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.compression.common.AdjacencyCompression;
import org.neo4j.gds.core.compression.common.ZigZagLongDecoding;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.gds.core.compression.common.ZigZagLongDecoding.Identity.INSTANCE;
import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.core.loading.ChunkedAdjacencyLists.NEXT_CHUNK_LENGTH;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ChunkedAdjacencyListsTest {

    @Test
    void shouldWriteSingleTargetList() {
        var adjacencyLists = ChunkedAdjacencyLists.of(0, 0);

        var input = new long[]{42L, 1337L, 5L};
        adjacencyLists.add(0, input, 0, 3, 3);

        var expectedTargets = new long[]{42L, 1337L, 5L};
        var actualTargets = new long[3];

        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.zigZagUncompressFrom(
            actualTargets,
            targets,
            length,
            position,
            INSTANCE
        ));
        assertThat(actualTargets).containsExactly(expectedTargets);
    }

    @Test
    void shouldWriteMultipleTimesIntoTargetList() {
        var adjacencyLists = ChunkedAdjacencyLists.of(0, 0);

        adjacencyLists.add(0, new long[]{42L, 1337L, 5L}, 0, 3, 3);
        adjacencyLists.add(0, new long[]{42L, 1337L, 5L}, 1, 3, 2);

        var expectedTargets = new long[]{42L, 1337L, 5L, 1337L, 5L};
        var actualTargets = new long[5];
        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.zigZagUncompressFrom(
            actualTargets,
            targets,
            length,
            position,
            INSTANCE
        ));
        assertThat(actualTargets).containsExactly(expectedTargets);
    }

    @Test
    void shouldWriteLargeAdjacencyListsWithOverflow() {
        var adjacencyLists = ChunkedAdjacencyLists.of(1, 0);

        var smallAdjacency = new long[37];
        Arrays.setAll(smallAdjacency, i -> i);

        var largeAdjacency = new long[NEXT_CHUNK_LENGTH[NEXT_CHUNK_LENGTH.length - 1] + 100];
        Arrays.setAll(largeAdjacency, i -> i + 42L);

        var smallProperties = new long[37];
        Arrays.setAll(smallProperties, i -> i);

        var largeProperties = new long[NEXT_CHUNK_LENGTH[NEXT_CHUNK_LENGTH.length - 1] + 100];
        Arrays.setAll(largeProperties, i -> i + 42L);

        adjacencyLists.add(
            0,
            Arrays.copyOf(smallAdjacency, smallAdjacency.length),
            new long[][]{smallProperties},
            0,
            smallAdjacency.length,
            smallAdjacency.length
        );
        adjacencyLists.add(
            0,
            Arrays.copyOf(smallAdjacency, smallAdjacency.length),
            new long[][]{smallProperties},
            0,
            smallAdjacency.length,
            smallAdjacency.length
        );
        adjacencyLists.add(
            0,
            Arrays.copyOf(largeAdjacency, largeAdjacency.length),
            new long[][]{largeProperties},
            0,
            largeAdjacency.length,
            largeAdjacency.length
        );

        var expectedTargets = new long[smallAdjacency.length * 2 + largeAdjacency.length];
        System.arraycopy(smallAdjacency, 0, expectedTargets, 0, smallAdjacency.length);
        System.arraycopy(smallAdjacency, 0, expectedTargets, smallAdjacency.length, smallAdjacency.length);
        System.arraycopy(largeAdjacency, 0, expectedTargets, smallAdjacency.length * 2, largeAdjacency.length);

        var expectedProperties = new long[smallProperties.length * 2 + largeProperties.length];
        System.arraycopy(smallProperties, 0, expectedProperties, 0, smallProperties.length);
        System.arraycopy(smallProperties, 0, expectedProperties, smallAdjacency.length, smallProperties.length);
        System.arraycopy(largeProperties, 0, expectedProperties, smallAdjacency.length * 2, largeProperties.length);

        var actualTargets = new long[smallAdjacency.length * 2 + largeAdjacency.length];
        adjacencyLists.consume((nodeId, targets, properties, position, length) -> {
            AdjacencyCompression.zigZagUncompressFrom(
                actualTargets,
                targets,
                length,
                position,
                INSTANCE
            );

            long[] actualProperties = new long[expectedProperties.length];
            var written = 0;
            for (long[] propertyChunk : properties[0]) {
                var valuesToCopy = Math.min(propertyChunk.length, length - written);
                System.arraycopy(propertyChunk, 0, actualProperties, written, valuesToCopy);
                written += valuesToCopy;
            }
            assertThat(Arrays.compare(expectedProperties, actualProperties)).isEqualTo(0);
        });
        assertThat(Arrays.compare(expectedTargets, actualTargets)).isEqualTo(0);
    }

    @Test
    void shouldWriteWithProperties() {
        var adjacencyLists = ChunkedAdjacencyLists.of(2, 0);

        var input = new long[]{42L, 1337L, 5L, 6L};
        var properties = new long[][]{{42L, 1337L, 5L, 6L}, {8L, 8L, 8L, 8L}};
        adjacencyLists.add(0, input, properties, 0, 4, 4);

        adjacencyLists.consume((nodeId, targets, actualProperties, position, length) -> {
            assertThat(actualProperties).hasNumberOfRows(2);
            assertThat(actualProperties[0]).hasNumberOfRows(1);
            assertThat(actualProperties[1]).hasNumberOfRows(1);

            assertThat(actualProperties[0][0]).containsSequence(42L, 1337L, 5L, 6L);
            assertThat(actualProperties[1][0]).containsSequence(8L, 8L, 8L, 8L);
        });
    }

    @Test
    void shouldWriteWithPropertiesWithOffset() {
        var adjacencyLists = ChunkedAdjacencyLists.of(2, 0);

        var input = new long[]{13L, 37L, 42L, 1337L, 5L, 6L};
        var properties = new long[][]{{0L, 0L, 42L, 1337L, 5L, 6L}, {0L, 0L, 8L, 8L, 8L, 8L}};
        adjacencyLists.add(0, input, properties, 2, 6, 4);

        adjacencyLists.consume((nodeId, targets, actualProperties, position, length) -> {
            assertThat(actualProperties).hasNumberOfRows(2);
            assertThat(actualProperties[0]).hasNumberOfRows(1);
            assertThat(actualProperties[1]).hasNumberOfRows(1);

            assertThat(actualProperties[0][0]).containsSequence(42L, 1337L, 5L, 6L);
            assertThat(actualProperties[1][0]).containsSequence(8L, 8L, 8L, 8L);
        });
    }

    @Test
    void shouldAllowConsumptionOfAllElements() {
        var adjacencyLists = ChunkedAdjacencyLists.of(0, 0);

        adjacencyLists.add(1, new long[]{42L, 1337L, 5L}, 0, 3, 3);
        adjacencyLists.add(8, new long[]{1L, 2L}, 0, 2, 2);

        // Skip 2 pages
        var largeIndex = 3 * 4096 + 1;
        adjacencyLists.add(largeIndex, new long[]{42L, 42L}, 0, 2, 2);

        adjacencyLists.consume((id, targets, properties, compressedBytesSize, compressedTargets) -> {
            assertThat(properties).isNull();

            var uncompressedTargets = new long[compressedTargets];
            ZigZagLongDecoding.zigZagUncompress(targets, compressedBytesSize, uncompressedTargets);

            if (id == 1) {
                assertThat(uncompressedTargets).containsExactly(42L, 1337L, 5L);
                assertThat(compressedBytesSize).isEqualTo(5);
                assertThat(compressedTargets).isEqualTo(3);
            } else if (id == 8) {
                assertThat(uncompressedTargets).containsExactly(1L, 2L);
                assertThat(compressedBytesSize).isEqualTo(2);
                assertThat(compressedTargets).isEqualTo(2);
            } else if (id == largeIndex) {
                assertThat(uncompressedTargets).containsExactly(42L, 42L);
                assertThat(compressedBytesSize).isEqualTo(2);
                assertThat(compressedTargets).isEqualTo(2);
            } else {
                fail(formatWithLocale("Did not expect to see node id %d", id));
            }
        });
    }

    @Test
    void addWithPreAggregation() {
        var adjacencyLists = ChunkedAdjacencyLists.of(0, 0);

        var input = new long[]{42L, IGNORE_VALUE, IGNORE_VALUE, 1337L, 5L};
        adjacencyLists.add(0, input, 0, 5, 3);

        var expectedTargets = new long[]{42L, 1337L, 5L};
        var actualTargets = new long[3];

        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.zigZagUncompressFrom(
            actualTargets,
            targets,
            length,
            position,
            INSTANCE
        ));
        assertThat(actualTargets).containsExactly(expectedTargets);
    }

    @Test
    void addWithPreAggregatedWeights() {
        var adjacencyLists = ChunkedAdjacencyLists.of(1, 0);

        var input = new long[]{42L, IGNORE_VALUE, 1337L, 5L};
        var properties = new long[][]{{3L, 2L, 3L, 4L}};
        adjacencyLists.add(0, input, properties, 0, 4, 3);

        var expectedTargets = new long[]{42L, 1337L, 5L};

        adjacencyLists.consume((nodeId, targets, actualProperties, position, length) -> {
            var actualTargets = new long[3];
            AdjacencyCompression.zigZagUncompressFrom(
                actualTargets,
                targets,
                length,
                position,
                INSTANCE
            );
            assertThat(actualTargets).containsExactly(expectedTargets);

            assertThat(actualProperties).hasNumberOfRows(1);
            assertThat(actualProperties[0]).hasNumberOfRows(1);
            assertThat(actualProperties[0][0]).containsSequence(3L, 3L, 4L);
        });
    }
}
