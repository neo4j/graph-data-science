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
package org.neo4j.graphalgo.beta.paths.yens;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.beta.paths.ImmutablePathResult;
import org.neo4j.graphalgo.beta.paths.PathResult;

import java.util.Arrays;

final class MutablePathResult {

    private long index;

    private final long sourceNode;

    private final long targetNode;

    private long[] nodeIds;

    private double[] costs;

    static MutablePathResult of(PathResult pathResult) {
        return new MutablePathResult(
            pathResult.index(),
            pathResult.sourceNode(),
            pathResult.targetNode(),
            pathResult.nodeIds(),
            pathResult.costs()
        );
    }

    private MutablePathResult(
        long index,
        long sourceNode,
        long targetNode,
        long[] nodeIds,
        double[] costs
    ) {
        this.index = index;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.nodeIds = nodeIds;
        this.costs = costs;
    }

    PathResult toPathResult() {
        return ImmutablePathResult.of(index, sourceNode, targetNode, nodeIds, costs);
    }

    MutablePathResult withIndex(int index) {
        this.index = index;
        return this;
    }

    int nodeCount() {
        return nodeIds.length;
    }

    long node(int index) {
        return nodeIds[index];
    }

    double totalCost() {
        return costs[costs.length - 1];
    }

    MutablePathResult subPath(int index) {
        return new MutablePathResult(
            index,
            sourceNode,
            targetNode,
            Arrays.copyOf(nodeIds, index),
            Arrays.copyOf(costs, index)
        );
    }

    boolean matches(MutablePathResult path, int index) {
        for (int i = 0; i < index; i++) {
            if (nodeIds[i] != path.nodeIds[i]) {
                return false;
            }
        }
        return true;
    }

    void append(MutablePathResult path) {
        // spur node is end of first and beginning of second path
        assert nodeIds[nodeIds.length - 1] == path.nodeIds[0];

        var oldLength = nodeIds.length;

        var newNodeIds = new long[oldLength + path.nodeIds.length - 1];
        var newCosts = new double[oldLength + path.nodeIds.length - 1];

        // copy node ids
        System.arraycopy(this.nodeIds, 0, newNodeIds, 0, oldLength);
        System.arraycopy(path.nodeIds, 1, newNodeIds, oldLength, path.nodeIds.length - 1);
        // copy costs
        System.arraycopy(this.costs, 0, newCosts, 0, oldLength);
        System.arraycopy(path.costs, 1, newCosts, oldLength, path.costs.length - 1);

        // add cost from previous path to each cost in the appended path
        var baseCost = newCosts[oldLength - 1];
        for (int i = oldLength; i < newCosts.length; i++) {
            newCosts[i] += baseCost;
        }

        this.nodeIds = newNodeIds;
        this.costs = newCosts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        return Arrays.equals(nodeIds, ((MutablePathResult) o).nodeIds);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(nodeIds);
    }

    @TestOnly
    long index() {
        return index;
    }
}
