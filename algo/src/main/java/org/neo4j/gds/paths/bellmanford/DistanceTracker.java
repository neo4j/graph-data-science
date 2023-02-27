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
package org.neo4j.gds.paths.bellmanford;


import org.neo4j.gds.core.utils.paged.DoublePageCreator;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.LongPageCreator;

import java.util.Optional;

class DistanceTracker {

    static double DIST_INF = Double.MAX_VALUE;
    static long NO_PREDECESSOR = Long.MAX_VALUE;
    static long NO_LENGTH = Long.MAX_VALUE;

    static DistanceTracker create(
        long size,
        int concurrency
    ) {
        var distances = HugeAtomicDoubleArray.newArray(
            size,
            DoublePageCreator.of(concurrency, index -> DIST_INF)
        );

        var predecessors = HugeAtomicLongArray.newArray(
            size,
            LongPageCreator.of(concurrency, index -> NO_PREDECESSOR)
        );
        var lengths = HugeAtomicLongArray.newArray(
            size,
            LongPageCreator.of(concurrency, index -> NO_LENGTH)
        );

        return new DistanceTracker(predecessors, distances, lengths);
    }

    private final HugeAtomicLongArray predecessors;
    // Use atomic array since it get/set methods are volatile
    private final HugeAtomicDoubleArray distances;

    private final HugeAtomicLongArray lengths;

    private DistanceTracker(
        HugeAtomicLongArray predecessors,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray lengths
    ) {
        this.predecessors = predecessors;
        this.distances = distances;
        this.lengths = lengths;
    }

    public double distance(long nodeId) {
        return distances.get(nodeId);
    }

    public long predecessor(long nodeId) {
        return predecessors.get(nodeId);
    }

    public long length(long nodeId) {return lengths.get(nodeId);}

    public HugeAtomicDoubleArray distances() {
        return distances;
    }

    public Optional<HugeAtomicLongArray> predecessors() {
        return Optional.of(predecessors);
    }

    public void set(long nodeId, long predecessor, double distance, long length) {
        distances.set(nodeId, distance);
        predecessors.set(nodeId, predecessor);
        lengths.set(nodeId, length);
    }

    public double compareAndExchange(
        long nodeId,
        double expectedDistance,
        double newDistance,
        long predecessor,
        long length
    ) {
        long currentPredecessor = predecessors.get(nodeId);

        // locked by another thread
        if (currentPredecessor < 0) {
            return distances.get(nodeId);
        }

        var witness = predecessors.compareAndExchange(nodeId, currentPredecessor, -predecessor - 1);

        // CAX failed
        if (witness != currentPredecessor) {
            return distances.get(nodeId);
        }

        // we have the look
        distances.set(nodeId, newDistance);
        lengths.set(nodeId, length);
        // unlock
        predecessors.set(nodeId, predecessor);

        // return previous distance to signal successful CAX
        return expectedDistance;
    }
}


