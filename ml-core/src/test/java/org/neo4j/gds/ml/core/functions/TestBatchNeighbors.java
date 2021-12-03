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
package org.neo4j.gds.ml.core.functions;

import com.carrotsearch.hppc.IntHashSet;
import org.neo4j.gds.ml.core.subgraph.BatchNeighbors;

import java.util.stream.IntStream;

class TestBatchNeighbors implements BatchNeighbors {

    private final int[][] neighbors;
    private final int[] batchIds;
    private final int numberOfNodes;

    TestBatchNeighbors(int[] batchIds, int[][] neighbors, int numberOfDistinctNodes) {
        this.neighbors = neighbors;
        this.batchIds = batchIds;
        this.numberOfNodes = numberOfDistinctNodes;
    }

    static TestBatchNeighbors of(int[] batchIds, int[][] neighborLists) {
        var distinctNodeIds = new IntHashSet();

       distinctNodeIds.addAll(batchIds);

        for (int[] neighbors : neighborLists) {
            distinctNodeIds.addAll(neighbors);
        }


        return new TestBatchNeighbors(batchIds, neighborLists, distinctNodeIds.size());
    }

    static TestBatchNeighbors of(int[][] neighbors) {
        return of(IntStream.range(0, neighbors.length).toArray(), neighbors);
    }

    @Override
    public int[] neighbors(int batchId) {
        return neighbors[batchId];
    }

    @Override
    public double relationshipWeight(int src, int trg) {
        return 1;
    }

    @Override
    public int[] batchIds() {
        return batchIds;
    }

    @Override
    public int batchSize() {
        return batchIds.length;
    }

    @Override
    public int nodeCount() {
        return numberOfNodes;
    }

    @Override
    public int degree(int batchId) {
        return neighbors[batchId].length;
    }
}
