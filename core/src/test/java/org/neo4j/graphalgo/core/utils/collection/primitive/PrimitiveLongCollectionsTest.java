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
package org.neo4j.graphalgo.core.utils.collection.primitive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimitiveLongCollectionsTest {

    private static final class CountingPrimitiveLongIteratorResource implements PrimitiveLongIterator, AutoCloseable {
        private final PrimitiveLongIterator delegate;
        private final AtomicInteger closeCounter;

        private CountingPrimitiveLongIteratorResource(PrimitiveLongIterator delegate, AtomicInteger closeCounter) {
            this.delegate = delegate;
            this.closeCounter = closeCounter;
        }

        @Override
        public void close() {
            closeCounter.incrementAndGet();
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public long next() {
            return delegate.next();
        }
    }

    @Test
    void singleWithDefaultMustAutoCloseEmptyIterator() {
        AtomicInteger counter = new AtomicInteger();
        CountingPrimitiveLongIteratorResource itr = new CountingPrimitiveLongIteratorResource(
            PrimitiveLongCollections.emptyIterator(), counter);
        assertEquals(PrimitiveLongCollections.single(itr, 2), 2);
        assertEquals(1, counter.get());
    }

    @Test
    void shouldDeduplicate() {
        // GIVEN
        long[] array = new long[]{1L, 1L, 2L, 5L, 6L, 6L};

        // WHEN
        long[] deduped = PrimitiveLongCollections.deduplicate(array);

        // THEN
        Assertions.assertArrayEquals(new long[]{1L, 2L, 5L, 6L}, deduped);
    }

    @Test
    void shouldNotContinueToCallNextOnHasNextFalse() {
        // GIVEN
        AtomicLong count = new AtomicLong(2);
        PrimitiveLongIterator iterator = new PrimitiveLongCollections.PrimitiveLongBaseIterator() {
            @Override
            protected boolean fetchNext() {
                return count.decrementAndGet() >= 0 && next(count.get());
            }
        };

        // WHEN/THEN
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(1L, iterator.next());
        assertTrue(iterator.hasNext());
        assertTrue(iterator.hasNext());
        assertEquals(0L, iterator.next());
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
        assertEquals(-1L, count.get());
    }
}
