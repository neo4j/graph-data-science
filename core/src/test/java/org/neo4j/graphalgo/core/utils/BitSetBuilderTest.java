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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class BitSetBuilderTest {

    @Test
    void testCorrectMergedBitset() {
        int chunkSize = 10;
        int globalBitSetSize = 1000;
        int numChunks = globalBitSetSize / chunkSize;

        BitSet[] localBitSets = new BitSet[numChunks];
        for (int i = 0; i < localBitSets.length; i++) {
           localBitSets[i] = new BitSet(chunkSize);
           if (i % 2 == 0) {
               localBitSets[i].set(0, chunkSize);
           }
        }

        BitSetBuilder bitSetBuilder = BitSetBuilder.of(globalBitSetSize, AllocationTracker.EMPTY);
        for (int i = 0; i < numChunks; i++) {
            bitSetBuilder.bulkAdd(chunkSize, localBitSets[i]);
        }
        BitSet finalBitSet = bitSetBuilder.build();
        int expectedSize = ((globalBitSetSize / Long.SIZE) + 1) * Long.SIZE;
        assertEquals(expectedSize, finalBitSet.capacity());

        boolean correctValues = true;
        for (int i = 0; i < globalBitSetSize; i++) {
            if (!correctValues) {
                break;
            }
            if ((i / chunkSize) % 2 == 0) {
                correctValues = finalBitSet.get(i);
            } else {
                correctValues = !finalBitSet.get(i);
            }
        }
        assertTrue(correctValues);
    }
}
