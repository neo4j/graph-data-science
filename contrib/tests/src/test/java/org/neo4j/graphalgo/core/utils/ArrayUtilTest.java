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
package org.neo4j.graphalgo.core.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ArrayUtilTest {

    final private int[] testData;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Integer> data() {
        return Arrays.asList(
                32, 33,
                64, 65,
                2048, 2049,
                4096, 4097
        );
    }

    public ArrayUtilTest(int size) {
        testData = new int[size];
        Arrays.setAll(testData, i -> (i + 1) * 2);
    }

    @Test
    public void testBinarySearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i * 2) + 1));
        }
    }

    @Test
    public void testLinearSearch() throws Exception {
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i * 2) + 1));
        }
    }
}