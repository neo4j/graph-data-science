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
package org.neo4j.graphalgo.core.utils.paged;

import java.util.stream.IntStream;

import static org.neo4j.graphalgo.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SHIFT;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;

public final class BytePageCreator {

    private final int concurrency;

    private BytePageCreator(int concurrency) {
        this.concurrency = concurrency;
    }

    public void fill(byte[][] pages, int lastPageSize) {
        int lastPageIndex = pages.length - 1;

        parallelStreamConsume(
            IntStream.range(0, lastPageIndex),
            concurrency,
            stream -> stream.forEach(pageIndex -> {
                createAndFillPage(pages, pageIndex, PAGE_SIZE);
            })
        );

        createAndFillPage(pages, lastPageIndex, lastPageSize);
    }

    public void fillPage(byte[] page, long base) {
        for (var i = 0; i < page.length; i++) {
            page[i] = (byte)(i + base);
        }
    }

    private void createAndFillPage(byte[][] pages, int pageIndex, int pageSize) {
        var page = new byte[pageSize];
        pages[pageIndex] = page;

        long base = ((long) pageIndex) << PAGE_SHIFT;
        fillPage(page, base);
    }

    public static BytePageCreator of(int concurrency) {
        return new BytePageCreator(concurrency);
    }
}
