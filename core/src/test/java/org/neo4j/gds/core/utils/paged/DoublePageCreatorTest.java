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

class DoublePageCreatorTest {

    @Test
    void fillsPages() {
        var numPages = 3;
        var lastPageSize = 4;

        double[][] pages = new double[numPages][];

        DoublePageCreator.of(1, (d) -> d).fill(pages, lastPageSize);

        for (int pageIndex = 0; pageIndex < pages.length; pageIndex++) {
            double[] page = pages[pageIndex];

            if(pageIndex < numPages - 1) {
                assertEquals(PAGE_SIZE, page.length);
            } else {
                assertEquals(lastPageSize, page.length);
            }

            for (int indexInPage = 0; indexInPage < page.length; indexInPage++) {
                double value = page[indexInPage];
                assertEquals(indexInPage + (pageIndex * PAGE_SIZE), value);
            }
        }
    }
}
