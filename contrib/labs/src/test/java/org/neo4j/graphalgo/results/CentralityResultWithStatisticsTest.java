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
package org.neo4j.graphalgo.results;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.impl.results.HugeDoubleArrayResult;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CentralityResultWithStatisticsTest {

    @Test
    void doubleArrayResult() {
        CentralityResultWithStatistics result = CentralityResultWithStatistics.Builder
                .of(new HugeDoubleArrayResult(HugeDoubleArray.of(1, 2, 3, 4)));

        assertEquals(4.0, result.computeMax(), 0.01);
        assertEquals(10.0, result.computeL1Norm(), 0.01);
        assertEquals(5.477225575051661, result.computeL2Norm(), 0.01);
    }
}
