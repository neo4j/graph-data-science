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
package org.neo4j.graphalgo.core.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrayUtilTest {

    static Stream<Integer> testDataSizes() {
        return Stream.of(32, 33, 64, 65, 2048, 2049, 4096, 4097);
    }

    public int[] setup(int size) {
        int[] testData = new int[size];
        Arrays.setAll(testData, i -> (i + 1) * 2);
        return testData;
    }

    @ParameterizedTest(name = "testDataSize: {0}")
    @MethodSource("testDataSizes")
    void testBinarySearch(int size) {
        int[] testData = setup(size);
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.binarySearch(testData, testData.length, (i * 2) + 1));
        }
    }

    @ParameterizedTest(name = "testDataSize: {0}")
    @MethodSource("testDataSizes")
    void testLinearSearch(int size) {
        int[] testData = setup(size);
        for (int i = 0; i < testData.length; i++) {
            assertTrue(String.format("False negative at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i + 1) * 2));
            assertFalse(String.format("False positive at %d value %d%n", i, testData[i]), ArrayUtil.linearSearch(testData, testData.length, (i * 2) + 1));
        }
    }
}