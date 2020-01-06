/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class RleReaderPropertyBasedTest {

    @Property
    void mixedRepeats(
            @ForAll List<@IntRange(min = 0, max = 2) Number> vector1List,
            @ForAll @IntRange(min = 1, max = 3) int limit
    ) {
        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, limit);
        System.out.println(vector1List);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        RleReader rleReader = new RleReader(vector1List.size());
        rleReader.reset(vector1Rle);
        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);
    }

}
