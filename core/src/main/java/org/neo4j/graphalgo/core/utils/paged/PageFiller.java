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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;

public class PageFiller implements Consumer<long[]>, BiConsumer<long[], Long> {

    private final int concurrency;
    private final LongUnaryOperator gen;

    PageFiller(int concurrency, LongUnaryOperator gen) {
        this.concurrency = concurrency;
        this.gen = gen;
    }

    @Override
    public void accept(long[] page) {
        accept(page, 0L);
    }

    @Override
    public void accept(long[] page, Long offset) {
        parallelStreamConsume(
            IntStream.range(0, page.length),
            concurrency,
            stream -> stream.forEach(i -> page[i] = gen.applyAsLong(i + offset))
        );
    }

    public static PageFiller of(int concurrency, LongUnaryOperator gen) {
        return new PageFiller(concurrency, gen);
    }

    public static PageFiller longToDouble(int concurrency, LongToDoubleFunction gen) {
        return new PageFiller(concurrency, convertLongDoubleFunctionToLongUnary(gen));
    }

    public static PageFiller allZeros(int concurrency) {
        return new PageFiller(concurrency, i -> 0L);
    }

    public static PageFiller identity(int concurrency) {
        return new PageFiller(concurrency, i -> i);
    }

    public static PageFiller passThrough() {
        return new PassThroughFiller();
    }

    private static LongUnaryOperator convertLongDoubleFunctionToLongUnary(LongToDoubleFunction gen) {
        return (index) -> Double.doubleToLongBits(gen.applyAsDouble(index));
    }

    private static class PassThroughFiller extends PageFiller {
        PassThroughFiller() {
            super(0, l -> 0L);
        }

        @Override
        public void accept(long[] page, Long offset) {
            // NOOP
        }
    }
}
