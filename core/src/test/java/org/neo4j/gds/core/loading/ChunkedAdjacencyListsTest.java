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

import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.core.loading.ZigZagLongDecoding.Identity.INSTANCE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ChunkedAdjacencyListsTest {

    @Test
    void shouldWriteSingleTargetList() {
        var adjacencyLists = ChunkedAdjacencyLists.of(0, 0);

        var input = new long[]{42L, 1337L, 5L};
        adjacencyLists.add(0, input, 0, 3, 3);

        var expectedTargets = new long[]{42L, 1337L, 5L};
        var actualTargets = new long[3];

        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.copyFrom(
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
        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.copyFrom(
            actualTargets,
            targets,
            length,
            position,
            INSTANCE
        ));
        assertThat(actualTargets).containsExactly(expectedTargets);
    }

    @Test
    void shouldWriteWithProperties() {
        var adjacencyLists = ChunkedAdjacencyLists.of(2, 0);

        var input = new long[]{42L, 1337L, 5L, 6L};
        var properties = new long[][]{{42L, 1337L, 5L, 6L}, {8L, 8L, 8L, 8L}};
        adjacencyLists.add(0, input, properties, 0, 4, 4);

        adjacencyLists.consume((nodeId, targets, actualProperties, position, length) -> assertThat(actualProperties)
            .hasDimensions(2, 4)
            .contains(new long[]{42L, 1337L, 5L, 6L}, Index.atIndex(0))
            .contains(new long[]{8L, 8L, 8L, 8L}, Index.atIndex(1)));
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

        adjacencyLists.consume((nodeId, targets, __, position, length) -> AdjacencyCompression.copyFrom(
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
            AdjacencyCompression.copyFrom(
                actualTargets,
                targets,
                length,
                position,
                INSTANCE
            );
            assertThat(actualTargets).containsExactly(expectedTargets);

            assertThat(actualProperties)
                // there is an additional entry, because we double the buffers in size
                .hasDimensions(1, 4).contains(new long[]{3L, 3L, 4L, 0L}, Index.atIndex(0));
        });
    }
}
