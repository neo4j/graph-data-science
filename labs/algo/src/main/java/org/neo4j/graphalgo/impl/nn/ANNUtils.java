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
package org.neo4j.graphalgo.impl.nn;

import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.roaringbitmap.RoaringBitmap;

import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class ANNUtils {
    public static long[] sampleNeighbors(long[] potentialNeighbors, double initialSampleSize, Random random) {
        shuffleArray(potentialNeighbors, random);

        int sampleSize = (int) Math.min(initialSampleSize, potentialNeighbors.length);
        long[] sampled = new long[sampleSize];
        System.arraycopy(potentialNeighbors, 0, sampled, 0, sampleSize);
        return sampled;

    }

    private static void shuffleArray(final long[] array, final Random random) {
        int index;
        long temp;
        for (int i = array.length - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = array[index];
            array[index] = array[i];
            array[i] = temp;
        }
    }

    public static HugeGraph hugeGraph(IdsAndProperties nodes, Relationships hugeRels) {
        return HugeGraph.create(
                AllocationTracker.EMPTY,
                nodes.idMap(),
                nodes.properties(),
                hugeRels.relationshipCount(),
                hugeRels.inAdjacency(),
                hugeRels.outAdjacency(),
                hugeRels.inOffsets(),
                hugeRels.outOffsets(),
                hugeRels.maybeDefaultRelProperty(),
                Optional.ofNullable(hugeRels.inRelProperties()),
                Optional.ofNullable(hugeRels.outRelProperties()),
                Optional.ofNullable(hugeRels.inRelPropertyOffsets()),
                Optional.ofNullable(hugeRels.outRelPropertyOffsets()),
                false);
    }

    static RoaringBitmap[] initializeRoaringBitmaps(int length) {
        RoaringBitmap[] tempVisitedRelationships = new RoaringBitmap[length];
        for (int i = 0; i < length; i++) {
            tempVisitedRelationships[i] = new RoaringBitmap();
        }
        return tempVisitedRelationships;
    }

    public static Set<Integer> selectRandomNeighbors(
            final int topK, final int numberOfInputs, final int excludeIndex,
            final Random random) {
        Set<Integer> randomNeighbors = new HashSet<>();
        while(randomNeighbors.size() < topK && randomNeighbors.size() < numberOfInputs - 1) {
            int index = random.nextInt(numberOfInputs);
            if (index != excludeIndex) {
                randomNeighbors.add(index);
            }
        }
        return randomNeighbors;
    }
}
