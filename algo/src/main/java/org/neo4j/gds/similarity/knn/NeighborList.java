/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.similarity.knn;

import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.similarity.SimilarityResult;

import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfLongArray;

class NeighborList {

    static MemoryEstimation memoryEstimation(int capacity) {
        return MemoryEstimations.builder(NeighborList.class)
            .fixed("elements", sizeOfLongArray(2L * capacity))
            .build();
    }

    /**
     * Unset the checked status from a value.
     *
     * We use the left-most/sign bit to carry the checked status.
     *
     * {@link Long#MAX_VALUE} is the largest positive long value,
     * so it has 0 in the left-most bit and all others set to 1.
     * {@code &}-ing that with a value will keep all lower 63 from the input and clear the left-most bit.
     */
    static long clearCheckedFlag(long value) {
        return value & Long.MAX_VALUE;
    }

    /**
     * Set the checked status from a value.
     *
     * We use the left-most/sign bit to carry the checked status.
     *
     * {@link Long#MIN_VALUE} is the smallest negative long value,
     * in two's complement is has a 1 in the left-most bit and all others set to 0.
     * {@code |}-ing that with a value will use all lower 63 from the input and always set the left-most bit.
     */
    private static long setCheckedFlag(long value) {
        return value | Long.MIN_VALUE;
    }

    /**
     * Query the checked status from a value.
     *
     * We use the left-most/sign bit to carry the checked status, so checking for that bit
     * is the same as checking for the sign.
     */
    static boolean isChecked(long value) {
        return value < 0;
    }

    /**
     * see {@link #add(long, double, java.util.SplittableRandom)} for an explanation on
     * why we use these constants and not booleans.
     */
    static final int NOT_INSERTED = 0;
    private static final int INSERTED = 1;

    // maximum number of elements, aka the top K
    private final int elementCapacity;
    // currently stored number of elements
    private int elementCount = 0;
    // we actually store tuples of (double, long), but the lack of inline classes forces us to
    // convert the double to their long bits representation and store them next to each other
    // every item occupies two entries in the array, [ doubleToLongBits(priority), element ]
    private final long[] priorityElementPairs;

    NeighborList(int elementCapacity) {
        if (elementCapacity <= 0) {
            throw new IllegalArgumentException("Bound cannot be smaller than or equal to 0");
        }

        this.elementCapacity = elementCapacity;
        this.priorityElementPairs = new long[elementCapacity * 2];
    }

    public LongStream elements() {
        return IntStream.range(0, elementCount).mapToLong(index -> priorityElementPairs[index * 2 + 1]);
    }

    public int size() {
        return elementCount;
    }

    long elementAt(int index) {
        return priorityElementPairs[index * 2 + 1];
    }

    long getAndFlagAsChecked(int index) {
        var element = priorityElementPairs[index * 2 + 1];
        priorityElementPairs[index * 2 + 1] = setCheckedFlag(element);
        return element;
    }

    /**
     * Tries to add the given element with the given priority to this list.
     *
     * This method and data structure is purpose-built for KNN, which counts the number
     * of insertions per round. To simplify that logic, we return 1 or 0 instead of true or false.
     * This allows KNN to just add the return values together without having the check on each of them.
     */
    public long add(long element, double priority, SplittableRandom random) {
        int insertIdx = 0;
        int currNumElementsWithPriority = elementCount * 2;

        if (elementCount != 0) {
            int lastValueIndex = (elementCount - 1) * 2;
            var lowestPriority = Double.longBitsToDouble(priorityElementPairs[lastValueIndex]);

            if (priority < lowestPriority && elementCount == elementCapacity) {
                return NOT_INSERTED;
            }

            int lowerBoundIdxInclusive = currNumElementsWithPriority;
            for (int i = 0; i < currNumElementsWithPriority; i += 2) {
                var storedPriority = Double.longBitsToDouble(priorityElementPairs[i]);
                if (priority >= storedPriority) {
                    lowerBoundIdxInclusive = i;
                    break;
                }
            }

            int upperBoundIdxExclusive = currNumElementsWithPriority;
            for (int i = lowerBoundIdxInclusive; i < currNumElementsWithPriority; i += 2) {
                var storedPriority = Double.longBitsToDouble(priorityElementPairs[i]);
                if (priority > storedPriority) {
                    upperBoundIdxExclusive = i;
                    break;
                }
            }

            int elementsWithPriorityCapacity = elementCapacity * 2;
            if (upperBoundIdxExclusive == elementsWithPriorityCapacity && lowestPriority == priority) {
                // TODO: Perturbation (maybe replace last element)
                return NOT_INSERTED;
            }

            if (lowerBoundIdxInclusive < currNumElementsWithPriority &&
                priority == Double.longBitsToDouble(priorityElementPairs[lowerBoundIdxInclusive])
            ) {
                var upperBound = Math.max(upperBoundIdxExclusive, lowerBoundIdxInclusive + 2);
                for (int i = lowerBoundIdxInclusive; i < upperBound; i += 2) {
                    if ((clearCheckedFlag(priorityElementPairs[i + 1])) == element) {
                        return NOT_INSERTED;
                    }
                }
            }

            if (lowerBoundIdxInclusive == upperBoundIdxExclusive) {
                insertIdx = lowerBoundIdxInclusive;
            } else {
                // if multiple entries have the same priority randomly chose the one to replace
                insertIdx = random.nextInt(lowerBoundIdxInclusive / 2, upperBoundIdxExclusive / 2) * 2;
            }

            if (insertIdx != lastValueIndex || elementCount != elementCapacity) {
                System.arraycopy(
                    priorityElementPairs,
                    insertIdx,
                    priorityElementPairs,
                    insertIdx + 2,
                    priorityElementPairs.length - insertIdx - 2
                );
            }
        }

        if (elementCount != elementCapacity) {
            elementCount++;
        }

        priorityElementPairs[insertIdx] = Double.doubleToRawLongBits(priority);
        priorityElementPairs[insertIdx + 1] = element;

        return INSERTED;
    }

    public Stream<SimilarityResult> similarityStream(long nodeId) {
        return IntStream.range(0, elementCount)
            .mapToObj(index -> {
                double neighborSimilarity = Double.longBitsToDouble(priorityElementPairs[index * 2]);
                long neighborId = clearCheckedFlag(priorityElementPairs[index * 2 + 1]);
                return new SimilarityResult(nodeId, neighborId, neighborSimilarity);
            });
    }
}
