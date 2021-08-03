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

import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.core.utils.mem.MemoryUsage.sizeOfLongArray;

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
    private final int bound;
    // currently stored number of elements
    private int elementCount = 0;
    // we actually store tuples of (double, long), but the lack of inline classes forces us to
    // convert the double to their long bits representation and store them next to each other
    // every item occupies two entries in the array, [ doubleToLongBits(priority), element ]
    private final long[] elements;

    NeighborList(int bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("Bound cannot be smaller than or equal to 0");
        }

        this.bound = bound;
        this.elements = new long[bound * 2];
    }

    public LongStream elements() {
        return IntStream.range(0, elementCount).mapToLong(index -> elements[index * 2 + 1]);
    }

    public int size() {
        return elementCount;
    }

    long elementAt(int index) {
        return elements[index * 2 + 1];
    }

    long getAndFlagAsChecked(int index) {
        var element = elements[index * 2 + 1];
        elements[index * 2 + 1] = setCheckedFlag(element);
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
        int insertPosition = 0;
        int allocatedElementsCount = elementCount * 2;

        if (elementCount != 0) {
            int lastValueIndex = (elementCount - 1) * 2;
            var lowestPriority = Double.longBitsToDouble(elements[lastValueIndex]);

            if (priority < lowestPriority && elementCount == bound) {
                return NOT_INSERTED;
            }

            int lowerBoundInclusive = allocatedElementsCount;
            for (int i = 0; i < allocatedElementsCount; i += 2) {
                var storedPriority = Double.longBitsToDouble(elements[i]);
                if (priority >= storedPriority) {
                    lowerBoundInclusive = i;
                    break;
                }
            }

            int upperBoundExclusive = allocatedElementsCount;
            for (int i = lowerBoundInclusive; i < allocatedElementsCount; i += 2) {
                var storedPriority = Double.longBitsToDouble(elements[i]);
                if (priority > storedPriority) {
                    upperBoundExclusive = i;
                    break;
                }
            }

            if (upperBoundExclusive == allocatedElementsCount && lowestPriority == priority) {
                // TODO: Perturbation
                return NOT_INSERTED;
            }

            if (lowerBoundInclusive < allocatedElementsCount && priority == Double.longBitsToDouble(elements[lowerBoundInclusive])) {
                var upperBound = Math.max(upperBoundExclusive, lowerBoundInclusive + 2);
                for (int i = lowerBoundInclusive; i < upperBound; i += 2) {
                    if ((clearCheckedFlag(elements[i + 1])) == element) {
                        return NOT_INSERTED;
                    }
                }
            }

            if (lowerBoundInclusive == upperBoundExclusive) {
                insertPosition = lowerBoundInclusive;
            } else {
                insertPosition = random.nextInt(lowerBoundInclusive / 2, upperBoundExclusive / 2) * 2;
            }

            if (insertPosition != lastValueIndex || elementCount != bound) {
                System.arraycopy(
                    elements,
                    insertPosition,
                    elements,
                    insertPosition + 2,
                    elements.length - insertPosition - 2
                );
            }
        }

        if (elementCount != bound) {
            elementCount++;
        }

        elements[insertPosition] = Double.doubleToRawLongBits(priority);
        elements[insertPosition + 1] = element;

        return INSERTED;
    }

    public Stream<SimilarityResult> similarityStream(long nodeId) {
        return IntStream.range(0, elementCount)
            .mapToObj(index -> {
                double neighborSimilarity = Double.longBitsToDouble(elements[index * 2]);
                long neighborId = clearCheckedFlag(elements[index * 2 + 1]);
                return new SimilarityResult(nodeId, neighborId, neighborSimilarity);
            });
    }
}
