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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.graphdb.Path;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class PathFindingStreamResult {

    public static final String COST_PROPERTY_NAME = "cost";

    public long index;

    public long sourceNode;

    public long targetNode;

    public double totalCost;

    public List<Long> nodeIds;

    public List<Double> costs;

    public Path path;

    public PathFindingStreamResult(
        long index,
        long sourceNode,
        long targetNode,
        double totalCost,
        List<Long> nodeIds,
        List<Double> costs,
        Path path
    ) {
        this.index = index;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.totalCost = totalCost;
        this.nodeIds = nodeIds;
        this.costs = costs;
        this.path = path;
    }

    public static class Builder {
        private final IdMap idMap;
        private final NodeLookup nodeLookup;

        public Builder(IdMap idMap, NodeLookup nodeLookup) {
            this.idMap = idMap;
            this.nodeLookup = nodeLookup;
        }

        public PathFindingStreamResult build(PathResult pathResult, boolean createCypherPath) {
            var nodeIds = pathResult.nodeIds();
            var costs = pathResult.costs();
            var pathIndex = pathResult.index();


            // convert internal ids to Neo ids
            for (int i = 0; i < nodeIds.length; i++) {
                nodeIds[i] = idMap.toOriginalNodeId(nodeIds[i]);
            }

            Path path = null;
            if (createCypherPath) {
                    path = StandardStreamPathCreator.create(
                        nodeLookup,
                        nodeIds,
                        costs,
                        pathIndex
                    );


            }

            return new PathFindingStreamResult(
                pathIndex,
                idMap.toOriginalNodeId(pathResult.sourceNode()),
                idMap.toOriginalNodeId(pathResult.targetNode()),
                pathResult.totalCost(),
                // 😿
                Arrays.stream(nodeIds).boxed().collect(Collectors.toCollection(() -> new ArrayList<>(nodeIds.length))),
                Arrays.stream(costs).boxed().collect(Collectors.toCollection(() -> new ArrayList<>(costs.length))),
                path
            );
        }
    }
}
