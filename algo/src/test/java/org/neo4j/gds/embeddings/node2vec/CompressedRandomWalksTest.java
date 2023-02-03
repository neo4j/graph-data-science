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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressedRandomWalksTest {

    @Test
    void shouldAddAndReadWalks() {
        var compressedRandomWalks = new CompressedRandomWalks(10);

        var walks = IntStream.range(0, 7).mapToObj(walkIndex -> {
            var walk = new long[walkIndex];
            for (int i = 0; i < walkIndex; i++) {
                walk[i] = walkIndex + i;
            }
            return walk;
        }).collect(Collectors.toList());

        walks.forEach(walk -> compressedRandomWalks.add(Arrays.copyOf(walk, walk.length)));

        assertIteratorContent(compressedRandomWalks.iterator(0, 3), List.of(walks.get(0), walks.get(1), walks.get(2)));
        assertIteratorContent(compressedRandomWalks.iterator(3, 4), List.of(walks.get(3), walks.get(4), walks.get(5), walks.get(6)));
    }

    @Test
    void shouldFailIfIteratorRangeIsTooLarge() {
        var compressedRandomWalks = new CompressedRandomWalks(10);

        compressedRandomWalks.add(0L, 1L);

        assertThatThrownBy(() -> compressedRandomWalks.iterator(0, 2))
            .hasMessageContaining("chunk exceeds the number of stored random walks")
            .hasMessageContaining("0-1");
    }

    private void assertIteratorContent(Iterator<long[]> iterator, Iterable<long[]> expected) {
        var decompressedWalks = new ArrayList<long[]>();
        iterator.forEachRemaining(decompressedWalk -> {
            var filteredWalk = Arrays.stream(decompressedWalk).filter(v -> v != -1L).toArray();
            decompressedWalks.add(filteredWalk);
        });

        assertThat(decompressedWalks).containsExactlyElementsOf(expected);
    }
}
