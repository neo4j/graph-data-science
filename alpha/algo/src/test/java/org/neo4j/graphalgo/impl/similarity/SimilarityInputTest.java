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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.Pools;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.impl.similarity.SimilarityInput.extractInputIds;

class SimilarityInputTest {

    @Test
    void findOneItem() {
        CategoricalInput[] ids = new CategoricalInput[3];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids, Pools.MAXIMUM_CONCURRENCY), Collections.singletonList(5L));

        assertArrayEquals(indexes, new int[] {0});
    }

    @Test
    void findMultipleItems() {
        CategoricalInput[] ids = new CategoricalInput[5];
        ids[0] = new CategoricalInput(5, new long[]{});
        ids[1] = new CategoricalInput(6, new long[]{});
        ids[2] = new CategoricalInput(7, new long[]{});
        ids[3] = new CategoricalInput(8, new long[]{});
        ids[4] = new CategoricalInput(9, new long[]{});

        int[] indexes = SimilarityInput.indexes(extractInputIds(ids, Pools.MAXIMUM_CONCURRENCY), Arrays.asList(5L, 9L));

        assertArrayEquals(indexes, new int[] {0, 4});
    }

    @Test
    void missingItem() {
        assertThrows(IllegalArgumentException.class, () -> {
                    CategoricalInput[] ids = new CategoricalInput[5];
                    ids[0] = new CategoricalInput(5, new long[]{});
                    ids[1] = new CategoricalInput(6, new long[]{});
                    ids[2] = new CategoricalInput(7, new long[]{});
                    ids[3] = new CategoricalInput(8, new long[]{});
                    ids[4] = new CategoricalInput(9, new long[]{});

                    int[] indexes = SimilarityInput.indexes(extractInputIds(ids, Pools.MAXIMUM_CONCURRENCY), Collections.singletonList(10L));

                    assertArrayEquals(indexes, new int[] {});
                }, "Node ids [10] do not exist in node ids list"
        );
    }

    @Test
    void allMissing() {
        assertThrows(IllegalArgumentException.class, () -> {
                    CategoricalInput[] ids = new CategoricalInput[5];
                    ids[0] = new CategoricalInput(5, new long[]{});
                    ids[1] = new CategoricalInput(6, new long[]{});
                    ids[2] = new CategoricalInput(7, new long[]{});
                    ids[3] = new CategoricalInput(8, new long[]{});
                    ids[4] = new CategoricalInput(9, new long[]{});

                    int[] indexes = SimilarityInput.indexes(extractInputIds(ids, Pools.MAXIMUM_CONCURRENCY), Arrays.asList(10L, 11L, -1L, 29L));

                    assertArrayEquals(indexes, new int[]{});
                }, "Node ids [10, 11, -1, 29] do not exist in node ids list"
        );
    }

    @Test
    void someMissingSomeFound() {
        assertThrows(IllegalArgumentException.class, () -> {
                    CategoricalInput[] ids = new CategoricalInput[5];
                    ids[0] = new CategoricalInput(5, new long[]{});
                    ids[1] = new CategoricalInput(6, new long[]{});
                    ids[2] = new CategoricalInput(7, new long[]{});
                    ids[3] = new CategoricalInput(8, new long[]{});
                    ids[4] = new CategoricalInput(9, new long[]{});

                    int[] indexes = SimilarityInput.indexes(extractInputIds(ids, Pools.MAXIMUM_CONCURRENCY), Arrays.asList(10L, 5L, 7L, 12L));

                    assertArrayEquals(indexes, new int[]{0, 2});
                }, "Node ids [10, 12] do not exist in node ids list"
        );
    }
}