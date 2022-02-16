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
package org.neo4j.gds.paths.delta;


import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.DoublePageCreator;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.LongPageCreator;

import java.util.Optional;

public interface TentativeDistances {

    double DIST_INF = Double.MAX_VALUE;
    long NO_PREDECESSOR = Long.MAX_VALUE;

    double get(long nodeId);

    // not thread-safe
    void set(long nodeId, long predecessor, double distance);

    double compareAndExchange(long nodeId, double currentDistance, double newDistance, long predecessor);

    default long predecessor(long nodeId) {
        return -1;
    }

    HugeAtomicDoubleArray distances();

    default Optional<HugeAtomicLongArray> predecessors() {
        return Optional.empty();
    }

    static DistanceOnly distanceOnly(
        long size,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        var distances = HugeAtomicDoubleArray.newArray(
            size,
            DoublePageCreator.of(concurrency, index -> DIST_INF),
            allocationTracker
        );
        return new DistanceOnly(distances);
    }

    static DistanceAndPredecessor distanceAndPredecessors(
        long size,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        var distances = HugeAtomicDoubleArray.newArray(
            size,
            DoublePageCreator.of(concurrency, index -> DIST_INF),
            allocationTracker
        );

        var predecessors = HugeAtomicLongArray.newArray(
            size,
            LongPageCreator.of(concurrency, index -> NO_PREDECESSOR),
            allocationTracker
        );

        return new DistanceAndPredecessor(predecessors, distances);
    }

    class DistanceOnly implements TentativeDistances {

        private final HugeAtomicDoubleArray distances;

        public DistanceOnly(HugeAtomicDoubleArray distances) {this.distances = distances;}

        @Override
        public double get(long nodeId) {
            return distances.get(nodeId);
        }

        @Override
        public void set(long nodeId, long predecessor, double distance) {
            distances.set(nodeId, distance);
        }

        @Override
        public double compareAndExchange(long nodeId, double currentDistance, double newDistance, long predecessor) {
            return distances.compareAndExchange(nodeId, currentDistance, newDistance);
        }

        @Override
        public HugeAtomicDoubleArray distances() {
            return distances;
        }
    }

    class DistanceAndPredecessor implements TentativeDistances {

        private final HugeAtomicLongArray predecessors;
        // Use atomic array since it get/set methods are volatile
        private final HugeAtomicDoubleArray distances;

        public DistanceAndPredecessor(HugeAtomicLongArray predecessors, HugeAtomicDoubleArray distances) {
            this.predecessors = predecessors;
            this.distances = distances;
        }

        @Override
        public double get(long nodeId) {
            return distances.get(nodeId);
        }

        @Override
        public long predecessor(long nodeId) {
            return predecessors.get(nodeId);
        }

        @Override
        public HugeAtomicDoubleArray distances() {
            return distances;
        }

        @Override
        public Optional<HugeAtomicLongArray> predecessors() {
            return Optional.of(predecessors);
        }

        @Override
        public void set(long nodeId, long predecessor, double distance) {
            distances.set(nodeId, distance);
            predecessors.set(nodeId, predecessor);
        }

        @Override
        public double compareAndExchange(long nodeId, double currentDistance, double newDistance, long predecessor) {
            long currentPredecessor = predecessors.get(nodeId);

            // locked by another thread
            if (currentPredecessor < 0) {
                return currentPredecessor;
            }

            var witness = predecessors.compareAndExchange(nodeId, currentPredecessor, -predecessor);

            // CAX failed
            if (witness != currentPredecessor) {
                return witness;
            }

            // we have the look
            distances.set(nodeId, newDistance);

            // unlock
            predecessors.set(nodeId, predecessor);

            // return previous distance to signal successful CAX
            return currentDistance;
        }
    }
}
