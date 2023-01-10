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
package org.neo4j.gds.paths.yens;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;

import java.util.Arrays;

/**
 * Helper data structure for Yen's algorithm that allows us
 * to manipulate {@link org.neo4j.gds.paths.PathResult}s.
 */
final class MutablePathResult {

    private long index;

    private final long sourceNode;

    private final long targetNode;

    private long[] nodeIds;

    private long[] relationshipIds;

    private double[] costs;

    static MutablePathResult of(PathResult pathResult) {
        return new MutablePathResult(
            pathResult.index(),
            pathResult.sourceNode(),
            pathResult.targetNode(),
            pathResult.nodeIds(),
            pathResult.relationshipIds(),
            pathResult.costs()
        );
    }

    private MutablePathResult(
        long index,
        long sourceNode,
        long targetNode,
        long[] nodeIds,
        long[] relationshipIds,
        double[] costs
    ) {
        this.index = index;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.nodeIds = nodeIds;
        this.relationshipIds = relationshipIds;
        this.costs = costs;
    }

    PathResult toPathResult() {
        return ImmutablePathResult.of(index, sourceNode, targetNode, nodeIds, relationshipIds, costs);
    }

    /**
     * Changes the index field to the given value and returns the mutated instance.
     */
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

    long relationship(int index) {
        return relationshipIds[index];
    }

    double totalCost() {
        return costs[costs.length - 1];
    }

    /**
     * Returns the path from the start to the given index (exclusive).
     */
    MutablePathResult subPath(int index) {
        return new MutablePathResult(
            index,
            sourceNode,
            targetNode,
            Arrays.copyOf(nodeIds, index),
            Arrays.copyOf(relationshipIds, index - 1),
            Arrays.copyOf(costs, index)
        );
    }

    /**
     * Returns true, iff this path matches the given path up until the given index (exclusive).
     * Two paths match, if they have the same node ids, but not necessarily the same costs.
     */
    boolean matches(MutablePathResult path, int index) {
        for (int i = 0; i < index; i++) {
            if (nodeIds[i] != path.nodeIds[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true, iff this path matches the given path up until the given index (exclusive).
     * Two paths match, if they have the same nodes as well as the same relationship ids
     */
    boolean matchesExactly(MutablePathResult path, int index) {

        if (relationshipIds == null || path.relationshipIds == null) {
            return matches(path, index);
        }

        for (int i = 0; i < index; i++) {
            if (nodeIds[i] != path.nodeIds[i]) {
                return false;
            }
            if (i >= 1) {
                if (relationshipIds[i - 1] != path.relationshipIds[i - 1]) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Appends the given path to this path.
     *
     * The last node in this path, must match the first node in the given path.
     * This node will only appear once in the resulting path.
     * The cost value associated with the last value in this path, is added to
     * the costs for each node in the second path.
     */
    void append(MutablePathResult path) {
        // spur node is end of first and beginning of second path
        assert nodeIds[nodeIds.length - 1] == path.nodeIds[0];

        var oldLength = nodeIds.length;

        var newNodeIds = new long[oldLength + path.nodeIds.length - 1];
        var newCosts = new double[oldLength + path.nodeIds.length - 1];

        var oldRelationshipIdsLength = relationshipIds.length;
        var newRelationshipIds = new long[oldRelationshipIdsLength + path.relationshipIds.length];

        // copy node ids
        System.arraycopy(this.nodeIds, 0, newNodeIds, 0, oldLength);
        System.arraycopy(path.nodeIds, 1, newNodeIds, oldLength, path.nodeIds.length - 1);
        // copy relationship ids
        System.arraycopy(this.relationshipIds, 0, newRelationshipIds, 0, oldRelationshipIdsLength);
        System.arraycopy(path.relationshipIds, 0, newRelationshipIds, oldRelationshipIdsLength, path.relationshipIds.length);
        // copy costs
        System.arraycopy(this.costs, 0, newCosts, 0, oldLength);
        System.arraycopy(path.costs, 1, newCosts, oldLength, path.costs.length - 1);

        // add cost from previous path to each cost in the appended path
        var baseCost = newCosts[oldLength - 1];
        for (int i = oldLength; i < newCosts.length; i++) {
            newCosts[i] += baseCost;
        }

        this.nodeIds = newNodeIds;
        this.relationshipIds = newRelationshipIds;
        this.costs = newCosts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        var other = (MutablePathResult) o;
        return Arrays.equals(nodeIds, other.nodeIds) && Arrays.equals(relationshipIds, other.relationshipIds);
    }

    @Override
    public int hashCode() {
        int h = 5381;
        h += (h << 5) + Arrays.hashCode(nodeIds);
        h += (h << 5) + Arrays.hashCode(relationshipIds);
        return h;
    }

    @Override
    public String toString() {
        return "MutablePathResult{" +
               "index=" + index +
               ", sourceNode=" + sourceNode +
               ", targetNode=" + targetNode +
               ", nodeIds=" + Arrays.toString(nodeIds) +
               ", relationshipIds=" + Arrays.toString(relationshipIds) +
               ", costs=" + Arrays.toString(costs) +
               '}';
    }

    @TestOnly
    long index() {
        return index;
    }
}
