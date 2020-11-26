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
package org.neo4j.graphalgo.beta.paths;

import org.immutables.builder.Builder;
import org.immutables.value.Value;

import java.util.List;

@Value.Style(depluralize = true)
public final class PathResult {

    public static final PathResult EMPTY = new PathResult();

    public int index;

    public long sourceNode;

    public long targetNode;

    public double totalCost;

    public List<Long> nodeIds;

    public List<Double> costs;

    private PathResult() {}

    private PathResult(
        int index,
        long sourceNode,
        long targetNode,
        double totalCost,
        List<Long> nodeIds,
        List<Double> costs
    ) {
        this.index = index;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.totalCost = totalCost;
        this.nodeIds = nodeIds;
        this.costs = costs;
    }

    @Builder.Factory
    static PathResult pathResult(
        int index,
        long sourceNode,
        long targetNode,
        double totalCost,
        List<Long> nodeIds,
        List<Double> costs
    ) {
        return new PathResult(index, sourceNode, targetNode, totalCost, nodeIds, costs);
    }
}
