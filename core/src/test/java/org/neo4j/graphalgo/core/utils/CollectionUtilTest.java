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

package org.neo4j.graphalgo.core.utils;

import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;

class CollectionUtilTest {

    @Test
    void testEnumerateEmptyList() {
        List<IntObjectPair<Long>> pairs = CollectionUtil
            .<Long>enumerate(emptyList())
            .collect(Collectors.toList());
        assertEquals(emptyList(), pairs);
    }

    @Test
    void testEnumerateSomeCollection() {
        Collection<Long> items = LazyBatchCollection.of(42, 1, (start, length) -> start);
        List<IntObjectPair<Long>> pairs = CollectionUtil
            .enumerate(items)
            .collect(Collectors.toList());
        assertEquals(42, pairs.size());
        for (IntObjectPair<Long> pair : pairs) {
            assertEquals(pair.getOne(), pair.getTwo());
        }
    }
}
