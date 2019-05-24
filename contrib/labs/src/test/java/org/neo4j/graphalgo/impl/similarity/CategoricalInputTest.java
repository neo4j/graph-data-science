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
package org.neo4j.graphalgo.impl.similarity;

import org.junit.Test;
import org.neo4j.graphalgo.impl.results.SimilarityResult;

import static org.junit.Assert.*;

public class CategoricalInputTest {

    @Test
    public void overlapShowsSmallerSideFirst() {
        CategoricalInput one = new CategoricalInput(3, new long[]{1, 2, 3, 4});
        CategoricalInput two = new CategoricalInput(7, new long[]{1, 2, 3});

        SimilarityResult result = one.overlap(0.0, two);

        assertEquals(7, result.item1);
        assertEquals(3, result.item2);
        assertEquals(3, result.count1);
        assertEquals(4, result.count2);
        assertEquals(3, result.intersection);
        assertEquals(1.0, result.similarity, 0.01);
    }

    @Test
    public void overlapShouldNotInferReverseIfRequestedNotTo() {
        CategoricalInput one = new CategoricalInput(3, new long[]{1, 2, 3, 4});
        CategoricalInput two = new CategoricalInput(7, new long[]{1, 2, 3});

        SimilarityResult result = one.overlap(0.0, two, false);
        assertNull(result);
    }
}