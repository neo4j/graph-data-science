/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.similarity;

import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@RunWith(JUnitQuickcheck.class)
public class RleReaderTest {
    @Test
    public void nothingRepeats() throws Exception {
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
    public void everythingRepeats() throws Exception {
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
    public void mixedRepeats() throws Exception {
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
    public void readTheSameItemMultipleTimes() throws Exception {
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
