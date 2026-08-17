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
package org.neo4j.gds.core.loading.nodeproperties;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.NodeIdMapper;
import org.neo4j.gds.api.nodes.IdMap;
import org.neo4j.gds.collections.DrainingIterator;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

class InnerNodePropertiesBuilderTest {

    private static final int PAGE_SIZE = 4;
    private static final Predicate<String> SKIP_NULL = value -> value == null;

    private final Map<Long, String> remapped = new ConcurrentHashMap<>();

    private void remap(
        String[][] pages,
        Predicate<String> skipValue,
        NodeIdMapper toMappedNodeIdFn,
        long highestOriginalId,
        int concurrency
    ) {
        InnerNodePropertiesBuilder.remapToInternalIds(
            new DrainingIterator<>(pages, PAGE_SIZE),
            (value, mappedId) -> remapped.put(mappedId, value),
            skipValue,
            toMappedNodeIdFn,
            highestOriginalId,
            new Concurrency(concurrency)
        );
    }

    @Test
    void keysValuesByTheirMappedId() {
        var pages = new String[][]{{"a", "b", "c", "d"}};

        remap(pages, SKIP_NULL, neoId -> neoId + 100, 3, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(
            100L, "a", 101L, "b", 102L, "c", 103L, "d"
        ));
    }

    @Test
    void skipsNodesMissingFromTheMapping() {
        var pages = new String[][]{{"a", "b", "c", "d"}};

        remap(pages, SKIP_NULL, neoId -> neoId % 2 == 0 ? neoId : IdMap.NOT_FOUND, 3, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(0L, "a", 2L, "c"));
    }

    @Test
    void skipsValuesRejectedByThePredicate() {
        var pages = new String[][]{{"keep", null, "default", "alsoKeep"}};

        remap(pages, value -> value == null || value.equals("default"), NodeIdMapper.IDENTITY, 3, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(0L, "keep", 3L, "alsoKeep"));
    }

    @Test
    void derivesOriginalIdsFromThePageOffset() {
        var pages = new String[][]{
            {"a", "b", "c", "d"},
            {"e", "f", "g", "h"}
        };

        remap(pages, SKIP_NULL, NodeIdMapper.IDENTITY, 7, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(
            0L, "a", 1L, "b", 2L, "c", 3L, "d",
            4L, "e", 5L, "f", 6L, "g", 7L, "h"
        ));
    }

    @Test
    void anAbsentPageDoesNotShiftTheIdsBehindIt() {
        var pages = new String[][]{
            {"a", "b", "c", "d"},
            null,
            {"i", "j", "k", "l"}
        };

        remap(pages, SKIP_NULL, NodeIdMapper.IDENTITY, 11, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(
            0L, "a", 1L, "b", 2L, "c", 3L, "d",
            8L, "i", 9L, "j", 10L, "k", 11L, "l"
        ));
    }

    @Test
    void ignoresValuesBeyondTheHighestOriginalId() {
        var pages = new String[][]{{"a", "b", "c", "d"}};

        remap(pages, SKIP_NULL, NodeIdMapper.IDENTITY, 1, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(0L, "a", 1L, "b"));
    }

    @Test
    void aPartiallyFilledTrailingPageIsClampedNotTruncated() {
        var pages = new String[][]{
            {"a", "b", "c", "d"},
            {"e", "f", "g", "h"}
        };

        remap(pages, SKIP_NULL, NodeIdMapper.IDENTITY, 5, 1);

        assertThat(remapped).containsExactlyInAnyOrderEntriesOf(Map.of(
            0L, "a", 1L, "b", 2L, "c", 3L, "d",
            4L, "e", 5L, "f"
        ));
    }

    @Test
    void drainsEveryValueExactlyOnceWhenRunConcurrently() {
        var pageCount = 64;
        var pages = new String[pageCount][];
        for (int page = 0; page < pageCount; page++) {
            pages[page] = new String[PAGE_SIZE];
            for (int index = 0; index < PAGE_SIZE; index++) {
                pages[page][index] = "v" + (page * PAGE_SIZE + index);
            }
        }
        var duplicates = new AtomicInteger();

        InnerNodePropertiesBuilder.remapToInternalIds(
            new DrainingIterator<>(pages, PAGE_SIZE),
            (value, mappedId) -> {
                if (remapped.put(mappedId, value) != null) {
                    duplicates.incrementAndGet();
                }
            },
            SKIP_NULL,
            NodeIdMapper.IDENTITY,
            pageCount * PAGE_SIZE - 1,
            new Concurrency(4)
        );

        assertThat(duplicates).hasValue(0);
        assertThat(remapped).hasSize(pageCount * PAGE_SIZE);
        assertThat(remapped.get(0L)).isEqualTo("v0");
        assertThat(remapped.get((long) pageCount * PAGE_SIZE - 1)).isEqualTo("v" + (pageCount * PAGE_SIZE - 1));
    }
}