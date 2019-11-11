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


import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class RleReaderTest {

    @Test
    void nothingRepeats() {
        // given
        List<Number> vector1List = asList(5.0, 4.0, 5.0, 4.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);

        // then
        RleReader rleReader = new RleReader(vector1List.size());
        rleReader.reset(vector1Rle);

        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);
    }

    @Test
    void everythingRepeats() {
        // given
        List<Number> vector1List = asList(5.0, 5.0, 5.0, 5.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);

        // then
        RleReader rleReader = new RleReader(vector1List.size());
        rleReader.reset(vector1Rle);

        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);
    }

    @Test
    void mixedRepeats() {
        // given
        List<Number> vector1List = asList(5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        RleReader rleReader = new RleReader(vector1List.size());
        rleReader.reset(vector1Rle);

        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);
    }

    @Test
    void readTheSameItemMultipleTimes() {
        // given
        List<Number> vector1List = asList(5.0, 5.0, 5.0, 5.0, 5.0, 4.0, 5.0);

        // when
        double[] vector1Rle = Weights.buildRleWeights(vector1List, 3);
        System.out.println(Arrays.toString(vector1Rle));

        // then
        RleReader rleReader = new RleReader(vector1List.size());

        rleReader.reset(vector1Rle);
        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);

        rleReader.reset(vector1Rle);
        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);

        rleReader.reset(vector1Rle);
        assertArrayEquals(vector1List.stream().mapToDouble(Number::doubleValue).toArray(), rleReader.read(), 0.01);
    }

}
