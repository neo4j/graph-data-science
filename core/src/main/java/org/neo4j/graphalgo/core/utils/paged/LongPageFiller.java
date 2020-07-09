/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.paged;

import java.util.function.Consumer;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;

public class LongPageFiller implements Consumer<long[]>, ObjLongConsumer<long[]> {

    private final int concurrency;
    private final LongUnaryOperator gen;

    LongPageFiller(int concurrency, LongUnaryOperator gen) {
        this.concurrency = concurrency;
        this.gen = gen;
    }

    @Override
    public void accept(long[] page) {
        accept(page, 0L);
    }

    @Override
    public void accept(long[] page, long offset) {
        for (int i = 0; i < page.length; i++) {
          page[i] = gen.applyAsLong(i + offset);
        }
    }

    public static LongPageFiller of(int concurrency, LongUnaryOperator gen) {
        return new LongPageFiller(concurrency, gen);
    }

    public static LongPageFiller longToDouble(int concurrency, LongToDoubleFunction gen) {
        return new LongPageFiller(concurrency, convertLongDoubleFunctionToLongUnary(gen));
    }

    public static LongPageFiller allZeros(int concurrency) {
        return new LongPageFiller(concurrency, i -> 0L);
    }

    public static LongPageFiller identity(int concurrency) {
        return new LongPageFiller(concurrency, i -> i);
    }

    public static LongPageFiller passThrough() {
        return new PassThroughFiller();
    }

    private static LongUnaryOperator convertLongDoubleFunctionToLongUnary(LongToDoubleFunction gen) {
        return (index) -> Double.doubleToLongBits(gen.applyAsDouble(index));
    }

    private static class PassThroughFiller extends LongPageFiller {
        PassThroughFiller() {
            super(0, l -> 0L);
        }

        @Override
        public void accept(long[] page, long offset) {
            // NOOP
        }
    }
}
