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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.mem.HugeArrays;

import java.util.function.DoubleUnaryOperator;
import java.util.stream.IntStream;

import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStreamConsume;


public final class DoublePageCreator {

    private final int concurrency;
    private final DoubleUnaryOperator gen;

    private DoublePageCreator(int concurrency, DoubleUnaryOperator gen) {
        this.concurrency = concurrency;
        this.gen = gen;
    }

    public void fill(double[][] pages, int lastPageSize) {
        int lastPageIndex = pages.length - 1;

        parallelStreamConsume(
            IntStream.range(0, lastPageIndex),
            concurrency,
            stream -> stream.forEach(pageIndex -> {
                createAndFillPage(pages, pageIndex, HugeArrays.PAGE_SIZE);
            })
        );

        createAndFillPage(pages, lastPageIndex, lastPageSize);
    }

    public void fillPage(double[] page, long base) {
        if (gen != null) {
            for (var i = 0; i < page.length; i++) {
                page[i] = gen.applyAsDouble(i + base);
            }
        }
    }

    private void createAndFillPage(double[][] pages, int pageIndex, int pageSize) {
        var page = new double[pageSize];
        pages[pageIndex] = page;

        long base = ((long) pageIndex) << HugeArrays.PAGE_SHIFT;
        fillPage(page, base);
    }

    public static DoublePageCreator of(int concurrency, DoubleUnaryOperator gen) {
        return new DoublePageCreator(concurrency, gen);
    }

    public static DoublePageCreator identity(int concurrency) {
        return new DoublePageCreator(concurrency, i -> i);
    }

    public static DoublePageCreator passThrough(int concurrency) {
        return new DoublePageCreator(concurrency, null);
    }
}
