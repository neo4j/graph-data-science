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
package org.neo4j.gds.embeddings;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.paged.HugeSerialObjectMergeSort;

import java.util.Random;

public class ConsistentWeightedSampler {
    private static final double SCALE_FACTOR = 32.0;
    private static final long NO_SAMPLE_FOUND = -1;
    private final Random random;
    private final HugeObjectArray<CirclePoint> points;

    public ConsistentWeightedSampler(
        HugeDoubleArray segmentCenters,
        HugeDoubleArray probabilities,
        HugeLongArray nodeIds
    ) {
        this.random = new Random();
        this.points = HugeObjectArray.newArray(CirclePoint.class, nodeIds.size());
        this.points.setAll(idx -> ImmutableCirclePoint.of(
            nodeIds.get(idx),
            segmentCenters.get(nodeIds.get(idx)),
            probabilities.get(idx)
        ));
        HugeSerialObjectMergeSort.sort(CirclePoint.class, this.points, CirclePoint::segmentCenter);
    }

    @ValueClass
    interface CirclePoint {
        long nodeId();

        double segmentCenter();

        double probability();

        default boolean isCloseEnough(double query, double normalizedScaleFactor) {
            return dist(query, segmentCenter()) < probability() / normalizedScaleFactor;
        }
    }

    public long sample(long randomSeed) {
        long currentSeed = randomSeed;
        while (true) {
            random.setSeed(currentSeed);
            currentSeed = random.nextInt();
            double query = random.nextDouble();
            long searchResult = circularSearch(query);
            if (searchResult != NO_SAMPLE_FOUND) {
                return searchResult;
            }
        }
    }

    private long circularSearch(double query) {
        if (points.size() < 8) {
            return circularLinearSearch(query);
        } else {
            return circularBinarySearch(query);
        }
    }

    private static double dist(double x, double y) {
        if (x > y) {
            return Math.min(x - y, y - x + 1);
        } else {
            return Math.min(y - x, x - y + 1);
        }
    }

    private long circularLinearSearch(double query) {
        for (int idx = 0; idx < points.size(); idx++) {
            var point = points.get(idx);
            if (point.isCloseEnough(query, 2.0 * SCALE_FACTOR)) {
                return point.nodeId();
            }
        }

        return NO_SAMPLE_FOUND;
    }

    private long circularBinarySearch(double query) {
        long low = 0L;
        long high = points.size() - 1;
        long size = points.size();

        var lowPoint = points.get(low);
        if (lowPoint.isCloseEnough(query, 2.0 * SCALE_FACTOR)) {
            return lowPoint.nodeId();
        }
        var highPoint = points.get(high);
        if (highPoint.isCloseEnough(query, 2.0 * SCALE_FACTOR)) {
            return highPoint.nodeId();
        }

        while (low < high + (high < low ? size : 0) - 1) {
            long mid;
            if (high > low) {
                mid = (high + low) / 2;
                if (query < points.get(mid).segmentCenter()) {
                    high = mid;
                } else {
                    low = mid;
                }
            } else {
                mid = Math.floorMod((high + low + size) / 2, size);
                var midHashTranslated = points.get(mid).segmentCenter() + (mid < low ? 1 : 0);
                var queryTranslated = query + (query < points.get(low).segmentCenter() ? 1 : 0);
                if (queryTranslated < midHashTranslated) {
                    high = mid;
                } else {
                    low = mid;
                }
            }
            var midPoint = points.get(mid);
            if (midPoint.isCloseEnough(query, 2.0 * SCALE_FACTOR)) {
                return midPoint.nodeId();
            }
        }

        return NO_SAMPLE_FOUND;
    }
}
