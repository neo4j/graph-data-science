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
package org.neo4j.gds.collections.primitive;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * Basic and common primitive long collection utils and manipulations.
 */
public final class PrimitiveLongCollections {

    private PrimitiveLongCollections() {
        throw new AssertionError("no instance");
    }

    // Range
    public static PrimitiveIterator.OfLong range(long start, long end) {
        return new PrimitiveLongRangeIterator(start, end);
    }

    /**
     * Base iterator for simpler implementations of {@link PrimitiveIterator.OfLong}s.
     */
    public abstract static class PrimitiveLongBaseIterator implements PrimitiveIterator.OfLong {
        private boolean hasNextDecided;
        private boolean hasNext;
        protected long next;

        @Override
        public boolean hasNext() {
            if (!hasNextDecided) {
                hasNext = fetchNext();
                hasNextDecided = true;
            }
            return hasNext;
        }

        @Override
        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements in " + this);
            }
            hasNextDecided = false;
            return next;
        }

        /**
         * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
         * item was found, that value must have been set inside the implementation of this method
         * using {@link #next(long)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
         * This method returns {@code true} so that it can be used in short-hand conditionals
         * (TODO what are they called?), like:
         * <pre>
         * protected boolean fetchNext()
         * {
         *     return source.hasNext() ? next( source.next() ) : false;
         * }
         * </pre>
         *
         * @param nextItem the next item found.
         */
        protected boolean next(long nextItem) {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static class PrimitiveLongRangeIterator extends PrimitiveLongBaseIterator {
        private long current;
        private final long end;

        PrimitiveLongRangeIterator(long start, long end) {
            this.current = start;
            this.end = end;
        }

        @Override
        protected boolean fetchNext() {
            try {
                return current <= end && next(current);
            } finally {
                current++;
            }
        }
    }
}
