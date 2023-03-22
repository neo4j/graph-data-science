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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.mem.HugeArrays.PAGE_SIZE;

class ParallelBytePageCreatorTest {

    @Test
    void fillsPages() {
        var pageShift = 14;
        var numPages = 3;
        var lastPageSize = 4;

        byte[][] pages = new byte[numPages][];

        ParallelBytePageCreator.of(1).fill(pages, lastPageSize, pageShift);

        for (int pageIndex = 0; pageIndex < pages.length; pageIndex++) {
            byte[] page = pages[pageIndex];

            if (pageIndex < numPages - 1) {
                assertEquals(PAGE_SIZE, page.length);
            } else {
                assertEquals(lastPageSize, page.length);
            }

            for (byte value : page) {
                assertEquals((byte) 0, value);
            }
        }
    }
}
