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

import org.neo4j.gds.collections.haa.PageCreator;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.function.LongUnaryOperator;
import java.util.stream.IntStream;

import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStreamConsume;

public final class ParalleLongPageCreator implements PageCreator.LongPageCreator {

    private final int concurrency;
    private final LongUnaryOperator gen;

    private ParalleLongPageCreator(int concurrency, LongUnaryOperator gen) {
        this.concurrency = concurrency;
        this.gen = gen;
    }

    public void fill(long[][] pages, int lastPageSize, int pageShift) {
        int lastPageIndex = pages.length - 1;
        int pageSize = 1 << pageShift;

        parallelStreamConsume(
            IntStream.range(0, lastPageIndex),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            stream -> stream.forEach(pageIndex -> createAndFillPage(pages, pageIndex, pageSize, pageShift))
        );

        createAndFillPage(pages, lastPageIndex, lastPageSize, pageShift);
    }

    public void fillPage(long[] page, long base) {
        if (gen != null) {
            for (var i = 0; i < page.length; i++) {
                page[i] = gen.applyAsLong(i + base);
            }
        }
    }

    private void createAndFillPage(long[][] pages, int pageIndex, int pageSize, int pageShift) {
        var page = new long[pageSize];
        pages[pageIndex] = page;

        long base = ((long) pageIndex) << pageShift;
        fillPage(page, base);
    }

    public static ParalleLongPageCreator of(int concurrency, LongUnaryOperator gen) {
        return new ParalleLongPageCreator(concurrency, gen);
    }

    public static ParalleLongPageCreator identity(int concurrency) {
        return new ParalleLongPageCreator(concurrency, i -> i);
    }

    public static ParalleLongPageCreator passThrough(int concurrency) {
        return new ParalleLongPageCreator(concurrency, null);
    }
}
