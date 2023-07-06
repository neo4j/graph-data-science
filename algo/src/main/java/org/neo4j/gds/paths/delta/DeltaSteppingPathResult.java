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

import org.neo4j.gds.paths.PathResult;

import java.util.Arrays;

/*
    We want to use hand-rolled PathResult implementation instead of the generated ImmutablePathResult one.
    The reason is that the generated Immutable version uses `array.clone()` when accessing the `nodeIds` and `costs` arrays
    which creates additional allocations that we would like to avoid.
 */
public final class DeltaSteppingPathResult implements PathResult {

    private static final long[] EMPTY_ARRAY = new long[0];

    private final long index;
    private final long sourceNode;
    private final long targetNode;
    private final long[] nodeIds;
    private final double[] costs;

    DeltaSteppingPathResult(
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

    @Override
    public long index() {
        return index;
    }

    @Override
    public long sourceNode() {
        return sourceNode;
    }

    @Override
    public long targetNode() {
        return targetNode;
    }

    @Override
    public long[] nodeIds() {
        return nodeIds;
    }

    @Override
    public long[] relationshipIds() {
        return EMPTY_ARRAY;
    }

    @Override
    public double[] costs() {
        return costs;
    }

    @Override
    public String toString() {
        return "DeltaSteppingPathResult{" +
            "index=" + index +
            ", sourceNode=" + sourceNode +
            ", targetNode=" + targetNode +
            ", nodeIds=" + Arrays.toString(nodeIds) +
            ", costs=" + Arrays.toString(costs) +
            '}';
    }
}
