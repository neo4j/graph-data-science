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
package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Collection;
import java.util.function.IntToLongFunction;

import static org.junit.Assert.*;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class IdMapTest extends RandomizedTest {

    @Test
    public void shouldReturnSingleIteratorForLargeBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        long[] ids = addRandomIds(idMap);
        idMap.buildMappedIds(AllocationTracker.EMPTY);

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(100);
        assertEquals(1, iterables.size());

        assertIterables(idMap, ids, iterables);
    }

    @Test
    public void shouldReturnMultipleIteratorsForSmallBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        long[] ids = addRandomIds(idMap);
        idMap.buildMappedIds(AllocationTracker.EMPTY);

        int expectedBatches = ids.length / 3;
        if (ids.length % 3 != 0) {
            expectedBatches++;
        }

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(3);
        assertEquals(expectedBatches, iterables.size());

        assertIterables(idMap, ids, iterables);
    }

    @Test
    public void shouldFailForZeroBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        addRandomIds(idMap);
        idMap.buildMappedIds(AllocationTracker.EMPTY);

        try {
            idMap.batchIterables(0);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: 0", e.getMessage());
        }
    }

    @Test
    public void shouldFailForNegativeBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        addRandomIds(idMap);
        idMap.buildMappedIds(AllocationTracker.EMPTY);

        int batchSize = between(Integer.MIN_VALUE, -1);

        try {
            idMap.batchIterables(batchSize);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: " + batchSize, e.getMessage());
        }
    }

    private void assertIterables(
            final IdMap idMap,
            final long[] ids,
            final Collection<PrimitiveIntIterable> iterables) {
        int i = 0;
        for (PrimitiveIntIterable iterable : iterables) {
            PrimitiveIntIterator iterator = iterable.iterator();
            while (iterator.hasNext()) {
                int next = iterator.next();
                long id = ids[i];
                assertEquals(i++, next);
                assertEquals(id, idMap.toOriginalNodeId(next));
            }
        }
    }

    private long[] addRandomIds(final IdMap idMap) {
        LongHashSet seen = new LongHashSet();
        return addSomeIds(idMap, i -> {
            long id;
            do {
                id = between(42L, 1337L);
            } while (!seen.add(id));
            return id;
        });
    }

    private long[] addSomeIds(final IdMap idMap, IntToLongFunction newId) {
        int iterations = between(10, 20);
        long[] ids = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            ids[i] = newId.applyAsLong(i);
            idMap.add(ids[i]);
        }
        return ids;
    }
}
