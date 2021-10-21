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
package org.neo4j.gds.collections;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class PageUtilTest {

    @ParameterizedTest
    @CsvSource(value = {
        "32768,2,16384",
        "32768,4,8192",
        "32768,8,4096",
        "4096,2,2048",
        "4096,4,1024",
        "4096,8,512",
    })
    void pageSizeFor(int pageSizeInBytes, int sizeOfElement, int pageSize) {
        assertThat(PageUtil.pageSizeFor(pageSizeInBytes, sizeOfElement)).isEqualTo(pageSize);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "32768,10000,1",
        "32768,100000,4",
        "32768,1000000,31",
        "4096,10000,3",
        "4096,100000,25",
        "4096,1000000,245",
    })
    void numPagesFor(int pageSizeInBytes, int capacity, int numPages) {
        assertThat(PageUtil.numPagesFor(capacity, pageSizeInBytes)).isEqualTo(numPages);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "1000,11,2048000",
        "1000,12,4096000",
        "1000,15,32768000",
    })
    void capacityFor(int numPages, int pageShift, long capacity) {
        assertThat(PageUtil.capacityFor(numPages, pageShift)).isEqualTo(capacity);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "10000,11,4",
        "10000,12,2",
        "10000,15,0",
        "100000,11,48",
        "100000,12,24",
        "100000,15,3",
    })
    void pageIndex(int index, int pageShift, long pageIndex) {
        assertThat(PageUtil.pageIndex(index, pageShift)).isEqualTo(pageIndex);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "10000,2047,1808",
        "10000,4095,1808",
        "10000,32767,10000",
        "100000,2047,1696",
        "100000,4095,1696",
        "100000,32767,1696",
    })
    void indexInPage(int index, int pageMask, long indexInPage) {
        assertThat(PageUtil.indexInPage(index, pageMask)).isEqualTo(indexInPage);
    }


}
