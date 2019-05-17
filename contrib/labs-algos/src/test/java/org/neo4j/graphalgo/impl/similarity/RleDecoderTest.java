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

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class RleDecoderTest {
    @Test
    public void readTwoArrays() {
        RleDecoder rleDecoder = new RleDecoder(2);

        double[] item1 = Weights.buildRleWeights(Arrays.asList(4.0, 4.0), 1);
        double[] item2 = Weights.buildRleWeights(Arrays.asList(3.0, 3.0), 1);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);
    }

    @Test
    public void readSameArraysAgain() {
        RleDecoder rleDecoder = new RleDecoder(2);

        double[] item1 = Weights.buildRleWeights(Arrays.asList(4.0, 4.0), 1);
        double[] item2 = Weights.buildRleWeights(Arrays.asList(3.0, 3.0), 1);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0}, rleDecoder.item2(), 0.01);
    }

    @Test
    public void readDifferentArrays() {
        RleDecoder rleDecoder = new RleDecoder(5);

        double[] item1 = Weights.buildRleWeights(Arrays.asList(4.0, 4.0, 3.0, 1.5, 5.3), 1);
        double[] item2 = Weights.buildRleWeights(Arrays.asList(3.0, 3.0, 4.0, 2.0, 1.0), 1);
        double[] item3 = Weights.buildRleWeights(Arrays.asList(3.0, 4.0, 5.0, 5.0, 5.0), 1);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0, 3.0, 1.5, 5.3}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0, 4.0, 2.0, 1.0}, rleDecoder.item2(), 0.01);

        rleDecoder.reset(item1, item2);

        assertArrayEquals(new double[] {4.0, 4.0, 3.0, 1.5, 5.3}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0, 4.0, 2.0, 1.0}, rleDecoder.item2(), 0.01);

        rleDecoder.reset(item3, item2);

        assertArrayEquals(new double[] {3.0, 4.0, 5.0, 5.0, 5.0}, rleDecoder.item1(), 0.01);
        assertArrayEquals(new double[] {3.0, 3.0, 4.0, 2.0, 1.0}, rleDecoder.item2(), 0.01);
    }
}