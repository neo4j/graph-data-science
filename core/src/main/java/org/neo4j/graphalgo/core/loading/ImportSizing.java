/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;
import static org.neo4j.graphalgo.core.utils.BitUtil.previousPowerOfTwo;

public final class ImportSizing {

    // batch size is used to pre-size multiple arrays, so it must fit in an integer
    // 1B elements might even be too much as arrays need to be allocated with
    // a consecutive chunk of memory
    // possible idea: retry with lower batch sizes if alloc hits an OOM?
    private static final long MAX_PAGE_SIZE = previousPowerOfTwo(Integer.MAX_VALUE);

    // don't attempt to page if the page size would be less than this value
    private static final long MIN_PAGE_SIZE = 1024L;

    private static final String TOO_MANY_PAGES_REQUIRED =
            "Importing %d nodes would need %d arrays of %d-long nested arrays each, which cannot be created.";

    private final int totalThreads;
    private final int pageSize;
    private final int numberOfPages;

    private ImportSizing(int totalThreads, int pageSize, int numberOfPages) {
        this.totalThreads = totalThreads;
        this.pageSize = pageSize;
        this.numberOfPages = numberOfPages;
    }

    public static ImportSizing of(int concurrency, long nodeCount) {
        return determineBestThreadSize(nodeCount, concurrency);
    }

    private static ImportSizing determineBestThreadSize(long nodeCount, long targetThreads) {
        // try to get about 4 pages per importer thread
        long pageSize = ceilDiv(nodeCount, targetThreads << 2);

        // page size must be a power of two
        pageSize = previousPowerOfTwo(pageSize);

        // page size must fit in an integer
        pageSize = Math.min(MAX_PAGE_SIZE, pageSize);

        // don't import overly small pages
        pageSize = Math.max(MIN_PAGE_SIZE, pageSize);

        // determine the actual number of pages required
        long numberOfPages = ceilDiv(nodeCount, pageSize);

        // if we need too many pages, try to increase the page size
        while (numberOfPages > MAX_PAGE_SIZE && pageSize <= MAX_PAGE_SIZE) {
            pageSize <<= 1L;
            numberOfPages = ceilDiv(nodeCount, pageSize);
        }

        if (numberOfPages > MAX_PAGE_SIZE || pageSize > MAX_PAGE_SIZE) {
            throw new IllegalArgumentException(
                    String.format(TOO_MANY_PAGES_REQUIRED, nodeCount, numberOfPages, pageSize)
            );
        }

        // int casts are safe as all are < MAX_BATCH_SIZE
        return new ImportSizing(
                (int) targetThreads,
                (int) pageSize,
                (int) numberOfPages
        );
    }

    int numberOfThreads() {
        return totalThreads;
    }

    public int pageSize() {
        return pageSize;
    }

    public int numberOfPages() {
        return numberOfPages;
    }
}
