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


import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.LongPageCreator;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;

import java.util.Optional;

public interface TentativeDistances {

    double DIST_INF = Double.MAX_VALUE;
    long NO_PREDECESSOR = Long.MAX_VALUE;

    /**
     * Returns the distance for the given node or {@link Double#MAX_VALUE} if not set.
     */
    double distance(long nodeId);

    /**
     * Returns the predecessor for the given node or {@link Long#MAX_VALUE} if not set.
     */
    long predecessor(long nodeId);

    /**
     * Sets the distance and the predecessor for the given node.
     */
    void set(long nodeId, long predecessor, double distance);

    /**
     * Atomically updates the distance and the predecessor for the given node.
     *
     * The method returns the witness value, which is the value we saw when
     * attempting a store operation. If the witness value is the expected
     * distance, the update for both, distance and predecessor, was successful.
     */
    double compareAndExchange(long nodeId, double expectedDistance, double newDistance, long predecessor);

    HugeAtomicDoubleArray distances();

    Optional<HugeAtomicLongArray> predecessors();

    static DistanceOnly distanceOnly(
        long size,
        int concurrency
    ) {
        var distances = HugeAtomicDoubleArray.of(
            size,
            ParallelDoublePageCreator.of(concurrency, index -> DIST_INF)
        );
        return new DistanceOnly(distances);
    }

    static DistanceAndPredecessor distanceAndPredecessors(
        long size,
        int concurrency
    ) {
        var distances = HugeAtomicDoubleArray.of(
            size,
            ParallelDoublePageCreator.of(concurrency, index -> DIST_INF)
        );

        var predecessors = HugeAtomicLongArray.newArray(
            size,
            LongPageCreator.of(concurrency, index -> NO_PREDECESSOR)
        );

        return new DistanceAndPredecessor(predecessors, distances);
    }

    class DistanceOnly implements TentativeDistances {

        private final HugeAtomicDoubleArray distances;

        public DistanceOnly(HugeAtomicDoubleArray distances) {this.distances = distances;}

        @Override
        public double distance(long nodeId) {
            return distances.get(nodeId);
        }

        @Override
        public long predecessor(long nodeId) {
            return NO_PREDECESSOR;
        }

        @Override
        public void set(long nodeId, long predecessor, double distance) {
            distances.set(nodeId, distance);
        }

        @Override
        public double compareAndExchange(long nodeId, double expectedDistance, double newDistance, long predecessor) {
            return distances.compareAndExchange(nodeId, expectedDistance, newDistance);
        }

        @Override
        public HugeAtomicDoubleArray distances() {
            return distances;
        }

        @Override
        public Optional<HugeAtomicLongArray> predecessors() {
            return Optional.empty();
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
        public double distance(long nodeId) {
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
        public double compareAndExchange(long nodeId, double expectedDistance, double newDistance, long predecessor) {
            long currentPredecessor = predecessors.get(nodeId);

            // locked by another thread
            if (currentPredecessor < 0) {
                //we  should signal failure
                // for that we must be sure not to return the 'expectedDistance' by accident!
                //we hence return its negation (or -1 if ==0)
                return (Double.compare(expectedDistance, 0.0) == 0) ? -1.0 : -expectedDistance;
            }

            var witness = predecessors.compareAndExchange(nodeId, currentPredecessor, -predecessor - 1);

            // CAX failed
            if (witness != currentPredecessor) {
                //we  should signal failure
                // for that we must be sure not to return the 'expectedDistance' by accident!
                return (Double.compare(expectedDistance, 0.0) == 0) ? -1.0 : -expectedDistance;
            }

            // we have the lock; no-one else can write on nodeId at the moment.
            // Let us do a check if it makes sense to update

            double oldDistance = distances.get(nodeId);

            if (oldDistance > newDistance) {
                distances.set(nodeId, newDistance);
                // unlock
                predecessors.set(nodeId, predecessor);
                // return previous distance to signal successful CAX

                return expectedDistance;
            }
            predecessors.set(nodeId, currentPredecessor);
            //signal unsuccesful update
            //note that this unsuccesful update will be the last attempt
            return (Double.compare(expectedDistance, 0.0) == 0.0) ? -1.0 : -expectedDistance;
        }
    }
}
