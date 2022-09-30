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

import java.util.stream.IntStream;

import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStreamConsume;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;

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
                createPage(pages, pageIndex, PAGE_SIZE);
            })
        );

        createPage(pages, lastPageIndex, lastPageSize);
    }

    private void createPage(byte[][] pages, int pageIndex, int pageSize) {
        var page = new byte[pageSize];
        pages[pageIndex] = page;
    }

    public static BytePageCreator of(int concurrency) {
        return new BytePageCreator(concurrency);
    }
}
