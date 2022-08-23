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
package org.neo4j.gds.core.huge;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.AdjacencyCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class NodeFilteredAdjacencyCursorTest {

    AdjacencyCursor adjacencyCursor;

    @BeforeEach
    void setup() {
        var innerCursor = new TestAdjacencyCursor(LongStream.range(0, 10).boxed().collect(Collectors.toList()));
        var filterIdMap = new FilteredDirectIdMap(10, l -> l % 2 == 0);
        this.adjacencyCursor = new NodeFilteredAdjacencyCursor(innerCursor, filterIdMap);
    }

    @Test
    void shouldIterateWithFilter() {
        List<Long> actual = new ArrayList<>();
        while (adjacencyCursor.hasNextVLong()) {
            actual.add(adjacencyCursor.nextVLong());
        }
        assertThat(actual).containsExactly(0L, 2L, 4L, 6L, 8L);
    }

    @Test
    void shouldPeekWithFilter() {
        assertThat(adjacencyCursor.hasNextVLong()).isTrue();
        assertThat(adjacencyCursor.peekVLong()).isEqualTo(0L);
        adjacencyCursor.nextVLong();

        assertThat(adjacencyCursor.hasNextVLong()).isTrue();
        assertThat(adjacencyCursor.peekVLong()).isEqualTo(2L);
        adjacencyCursor.nextVLong();

        assertThat(adjacencyCursor.hasNextVLong()).isTrue();
        assertThat(adjacencyCursor.peekVLong()).isEqualTo(4L);
    }

    @Test
    void shouldSkipUntilWithFilter() {
        assertThat(adjacencyCursor.skipUntil(4L)).isEqualTo(6L);
        assertThat(adjacencyCursor.skipUntil(8L)).isEqualTo(AdjacencyCursor.NOT_FOUND);
    }

    @Test
    void shouldAdvanceWithFilter() {
        assertThat(adjacencyCursor.advance(6L)).isEqualTo(6L);
        assertThat(adjacencyCursor.advance(7L)).isEqualTo(8L);
        assertThat(adjacencyCursor.advance(9L)).isEqualTo(AdjacencyCursor.NOT_FOUND);
    }

    static class FilteredDirectIdMap extends DirectIdMap {

        private final LongPredicate nodeFilter;

        FilteredDirectIdMap(long nodeCount, LongPredicate nodeFilter) {
            super(nodeCount);
            this.nodeFilter = nodeFilter;
        }

        @Override
        public boolean contains(long originalNodeId) {
            return nodeFilter.test(originalNodeId);
        }
    }
}
