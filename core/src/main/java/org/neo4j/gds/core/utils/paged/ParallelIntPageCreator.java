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

import java.util.stream.IntStream;

import static org.neo4j.gds.core.concurrency.ParallelUtil.parallelStreamConsume;

public final class ParallelIntPageCreator implements PageCreator.IntPageCreator {

    private final int concurrency;

    public ParallelIntPageCreator(int concurrency) {
        this.concurrency = concurrency;
    }

    public void fill(int[][] pages, int lastPageSize, int pageShift) {
        int lastPageIndex = pages.length - 1;
        int pageSize = 1 << pageShift;

        parallelStreamConsume(
            IntStream.range(0, lastPageIndex),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            stream -> stream.forEach(pageIndex -> createPage(pages, pageIndex, pageSize))
        );

        createPage(pages, lastPageIndex, lastPageSize);
    }

    @Override
    public void fillPage(int[] page, long base) {
        // NO-OP
    }

    private void createPage(int[][] pages, int pageIndex, int pageSize) {
        var page = new int[pageSize];
        pages[pageIndex] = page;
    }

    public static ParallelIntPageCreator of(int concurrency) {
        return new ParallelIntPageCreator(concurrency);
    }
}
