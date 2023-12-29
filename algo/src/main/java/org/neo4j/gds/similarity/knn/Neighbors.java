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

import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.concurrent.atomic.LongAdder;

final class Neighbors {
    private final HugeObjectArray<NeighborList> neighbors;
    private LongAdder neighborsFound;
    private LongAdder joinCounter;

    Neighbors(long nodeCount) {
        this.neighbors = HugeObjectArray.newArray(NeighborList.class, nodeCount);
        this.neighborsFound = new LongAdder();
        this.joinCounter = new LongAdder();
    }

    Neighbors(HugeObjectArray<NeighborList> neighbors) {
        this.neighbors = neighbors;
    }

    NeighborList get(long nodeId) {
        return neighbors.get(nodeId);
    }

    NeighborList getAndIncrementCounter(long nodeId) {
        incrementJoinCounter();
        return get(nodeId);
    }
    void set(long nodeId, NeighborList neighborList) {
        neighbors.set(nodeId, neighborList);
        neighborsFound.add(neighborList.size());
    }

    long size() {
        return neighbors.size();
    }

    long neighborsFound() {
        return neighborsFound.longValue();
    }

    void incrementJoinCounter() {
        joinCounter.increment();
    }

    long joinCounter() {
        return joinCounter.longValue();
    }

    void filterHighSimilarityResult(long nodeId, double similarityCutoff) {
        neighbors.get(nodeId).filterHighSimilarityResults(similarityCutoff);
    }

    HugeObjectArray<NeighborList> data() {
        return neighbors;
    }
}
