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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;

import static org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveCommons.closeSafely;

/**
 * Basic and common primitive long collection utils and manipulations.
 */
public final class PrimitiveLongCollections {

    private static final PrimitiveLongIterator EMPTY = new PrimitiveLongBaseIterator() {
        @Override
        protected boolean fetchNext() {
            return false;
        }
    };

    private PrimitiveLongCollections() {
        throw new AssertionError("no instance");
    }

    // Concating
    public static PrimitiveLongIterator concat(PrimitiveLongIterator... primitiveLongIterators) {
        return concat(Arrays.asList(primitiveLongIterators));
    }

    public static PrimitiveLongIterator concat(Iterable<PrimitiveLongIterator> primitiveLongIterators) {
        return new PrimitiveLongConcatingIterator(primitiveLongIterators.iterator());
    }

    public static PrimitiveLongIterator filter(PrimitiveLongIterator source, final LongPredicate filter) {
        return new PrimitiveLongFilteringIterator(source) {
            @Override
            public boolean test(long item) {
                return filter.test(item);
            }
        };
    }

    // Range
    public static PrimitiveLongIterator range(long start, long end) {
        return new PrimitiveLongRangeIterator(start, end);
    }

    public static long single(PrimitiveLongIterator iterator, long defaultItem) {
        try {
            if (!iterator.hasNext()) {
                closeSafely(iterator, null);
                return defaultItem;
            }
            long item = iterator.next();
            if (iterator.hasNext()) {
                throw new NoSuchElementException("More than one item in " + iterator + ", first:" + item +
                                                 ", second:" + iterator.next());
            }
            closeSafely(iterator, null);
            return item;
        } catch (NoSuchElementException exception) {
            closeSafely(iterator, exception);
            throw exception;
        }
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item     the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static int indexOf(PrimitiveLongIterator iterator, long item) {
        for (int i = 0; iterator.hasNext(); i++) {
            if (item == iterator.next()) {
                return i;
            }
        }
        return -1;
    }

    public static int count(PrimitiveLongIterator iterator) {
        int count = 0;
        for (; iterator.hasNext(); iterator.next(), count++) {   // Just loop through this
        }
        return count;
    }

    public static PrimitiveLongIterator emptyIterator() {
        return EMPTY;
    }

    public static <T> Iterator<T> map(final LongFunction<T> mapFunction, final PrimitiveLongIterator source) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return source.hasNext();
            }

            @Override
            public T next() {
                return mapFunction.apply(source.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link List}, boxing each long.
     *
     * @param iterator {@link PrimitiveLongIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Long> asList(PrimitiveLongIterator iterator) {
        List<Long> out = new ArrayList<>();
        while (iterator.hasNext()) {
            out.add(iterator.next());
        }
        return out;
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link Set}, boxing each long.
     *
     * @param iterator {@link PrimitiveLongIterator} to pull values from.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet(PrimitiveLongIterator iterator) {
        Set<Long> set = new HashSet<>();
        while (iterator.hasNext()) {
            addUnique(set, iterator.next());
        }
        return set;
    }

    private static <T, C extends Collection<T>> void addUnique(C collection, T item) {
        if (!collection.add(item)) {
            throw new IllegalStateException("Encountered an already added item:" + item +
                                            " when adding items uniquely to a collection:" + collection);
        }
    }

    /**
     * Deduplicates values in the sorted {@code values} array.
     *
     * @param values sorted array of long values.
     * @return the provided array if no duplicates were found, otherwise a new shorter array w/o duplicates.
     */
    public static long[] deduplicate(long[] values) {
        int unique = 0;
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            for (int j = 0; j < unique; j++) {
                if (value == values[j]) {
                    value = -1; // signal that this value is not unique
                    break; // we will not find more than one conflict
                }
            }
            if (value != -1) {   // this has to be done outside the inner loop, otherwise we'd never accept a single one...
                values[unique++] = values[i];
            }
        }
        return unique < values.length ? Arrays.copyOf(values, unique) : values;
    }

    /**
     * Base iterator for simpler implementations of {@link PrimitiveLongIterator}s.
     */
    public abstract static class PrimitiveLongBaseIterator implements PrimitiveLongIterator {
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
        public long next() {
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

    public static class PrimitiveLongConcatingIterator extends PrimitiveLongBaseIterator {
        private final Iterator<? extends PrimitiveLongIterator> iterators;
        private PrimitiveLongIterator currentIterator;

        public PrimitiveLongConcatingIterator(Iterator<? extends PrimitiveLongIterator> iterators) {
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext() {
            if (currentIterator == null || !currentIterator.hasNext()) {
                while (iterators.hasNext()) {
                    currentIterator = iterators.next();
                    if (currentIterator.hasNext()) {
                        break;
                    }
                }
            }
            return (currentIterator != null && currentIterator.hasNext()) && next(currentIterator.next());
        }
    }

    public abstract static class PrimitiveLongFilteringIterator extends PrimitiveLongBaseIterator
        implements LongPredicate {
        protected final PrimitiveLongIterator source;

        PrimitiveLongFilteringIterator(PrimitiveLongIterator source) {
            this.source = source;
        }

        @Override
        protected boolean fetchNext() {
            while (source.hasNext()) {
                long testItem = source.next();
                if (test(testItem)) {
                    return next(testItem);
                }
            }
            return false;
        }

        @Override
        public abstract boolean test(long testItem);
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
