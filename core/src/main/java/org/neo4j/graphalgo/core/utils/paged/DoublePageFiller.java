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
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;

public class DoublePageFiller implements Consumer<double[]>, ObjLongConsumer<double[]> {

    private final int concurrency;
    private final DoubleUnaryOperator gen;

    DoublePageFiller(int concurrency, DoubleUnaryOperator gen) {
        this.concurrency = concurrency;
        this.gen = gen;
    }

    @Override
    public void accept(double[] page) {
        accept(page, 0L);
    }

    @Override
    public void accept(double[] page, long offset) {
        parallelStreamConsume(
            IntStream.range(0, page.length),
            concurrency,
            stream -> stream.forEach(i -> page[i] = gen.applyAsDouble(i + offset))
        );
    }

    public static DoublePageFiller of(int concurrency, DoubleUnaryOperator gen) {
        return new DoublePageFiller(concurrency, gen);
    }

    public static DoublePageFiller passThrough() {
        return new PassThroughFiller();
    }

    private static class PassThroughFiller extends DoublePageFiller {
        PassThroughFiller() {
            super(0, l -> 0L);
        }

        @Override
        public void accept(double[] page, long offset) {
            // NOOP
        }
    }
}
