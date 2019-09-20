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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.PAGE_SIZE;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.exclusiveIndexOfPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.numberOfPages;
import static org.neo4j.graphalgo.core.utils.paged.HugeArrays.pageIndex;

final class HugeArraysTest {

    @Test
    void pageIndexOfFirstPage() {
        assertEquals(0L, pageIndex(0L));
        assertEquals(0L, pageIndex(1L));
        assertEquals(0L, pageIndex(PAGE_SIZE - 1));
    }

    @Test
    void pageIndexOfSecondPage() {
        assertEquals(1L, pageIndex(PAGE_SIZE));
        assertEquals(1L, pageIndex(2 * PAGE_SIZE - 1));
    }

    @Test
    void pageIndexOfMaxPage() {
        assertEquals(Integer.MAX_VALUE, pageIndex((long) Integer.MAX_VALUE * PAGE_SIZE));
        assertEquals(Integer.MAX_VALUE, pageIndex((long) Integer.MAX_VALUE * PAGE_SIZE + PAGE_SIZE - 1L));
    }

    @Test
    void pageIndexUndefinedWhenIndexIsTooLarge() {
        assertEquals(0L, pageIndex(1L << 62));
        assertEquals(-1L, pageIndex(Long.MAX_VALUE));
    }

    @Test
    void pageIndexUndefinedForNegativeIndex() {
        assertEquals(-1L, pageIndex(-1L));
        assertEquals(-1L, pageIndex(-2L));
        assertEquals(0L, pageIndex(Long.MIN_VALUE));
    }

    @Test
    void indexInPageOnFirstPage() {
        assertEquals(0L, indexInPage(0L));
        assertEquals(1L, indexInPage(1L));
        assertEquals(PAGE_SIZE - 1L, indexInPage(PAGE_SIZE - 1L));
    }

    @Test
    void indexInPageOnSecondPage() {
        assertEquals(0L, indexInPage(PAGE_SIZE));
        assertEquals(1L, indexInPage(PAGE_SIZE + 1L));
        assertEquals(PAGE_SIZE - 1L, indexInPage(2 * PAGE_SIZE - 1L));
    }

    @Test
    void indexInPageOfMaxPage() {
        assertEquals(0L, indexInPage((long) Integer.MAX_VALUE * PAGE_SIZE));
        assertEquals(PAGE_SIZE - 1L, indexInPage((long) Integer.MAX_VALUE * PAGE_SIZE + PAGE_SIZE - 1L));
    }

    @Test
    void indexInPageWhenIndexIsTooLarge() {
        assertEquals(0L, indexInPage(1L << 62));
        assertEquals(PAGE_SIZE - 1L, indexInPage(Long.MAX_VALUE));
    }

    @Test
    void indexInPageForNegativeIndex() {
        assertEquals(PAGE_SIZE - 1L, indexInPage(-1L));
        assertEquals(PAGE_SIZE - 2L, indexInPage(-2L));
        assertEquals(0L, indexInPage(Long.MIN_VALUE));
    }

    @Test
    void exclusiveIndexOfPageOnFirstPage() {
        assertEquals(1L, exclusiveIndexOfPage(1L));
        assertEquals(PAGE_SIZE - 1L, exclusiveIndexOfPage(PAGE_SIZE - 1L));
    }

    @Test
    void exclusiveIndexDoesNotJumpToNextPage() {
        assertEquals(PAGE_SIZE, exclusiveIndexOfPage(0L));
        assertEquals(PAGE_SIZE, exclusiveIndexOfPage(PAGE_SIZE));
    }

    @Test
    void numberOfPagesForNoData() {
        assertEquals(0L, numberOfPages(0L));
    }

    @Test
    void numberOfPagesForOnePage() {
        assertEquals(1L, numberOfPages(1L));
        assertEquals(1L, numberOfPages(PAGE_SIZE));
    }

    @Test
    void numberOfPagesForMorePages() {
        assertEquals(2L, numberOfPages(PAGE_SIZE + 1L));
        assertEquals(2L, numberOfPages(2 * PAGE_SIZE));
    }

    @Test
    void numberOfPagesForMaxPages() {
        assertEquals(Integer.MAX_VALUE, numberOfPages((long) Integer.MAX_VALUE * PAGE_SIZE));
    }

    @Test
    void numberOfPagesForTooManyPages() {
        testNumberOfPagesForTooManyPages(Long.MAX_VALUE);
        testNumberOfPagesForTooManyPages((long) Integer.MAX_VALUE * PAGE_SIZE + 1L);
    }

    private void testNumberOfPagesForTooManyPages(final long capacity) {
        try {
            numberOfPages(capacity);
            fail("capacity should have been too large");
        } catch (AssertionError e) {
            assertEquals("pageSize=" + PAGE_SIZE + " is too small for capacity: " + capacity, e.getMessage());
        }
    }
}
