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
package org.neo4j.graphalgo.beta.paths;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.graphalgo.beta.paths.PathFactory.create;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@SuppressWarnings("unused")
public final class StreamResult {

    public static final String COST_PROPERTY_NAME = "cost";

    public long index;

    public long sourceNode;

    public long targetNode;

    public double totalCost;

    public List<Long> nodeIds;

    public List<Double> costs;

    public Path path;

    private StreamResult(
        long index,
        long sourceNode,
        long targetNode,
        double totalCost,
        List<Long> nodeIds,
        List<Double> costs,
        @Nullable Path path
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
        private long relationshipIdOffset;
        private final IdMapping idMapping;
        private final Transaction transaction;

        public Builder(IdMapping idMapping, Transaction transaction) {
            this.relationshipIdOffset = -1L;
            this.idMapping = idMapping;
            this.transaction = transaction;
        }

        public StreamResult build(PathResult pathResult, boolean createCypherPath) {
            var nodeIds = pathResult.nodeIds();
            var costs = pathResult.costs();
            var pathIndex = pathResult.index();

            var relationshipType = RelationshipType.withName(formatWithLocale("PATH_%d", pathIndex));

            Path path = null;
            if (createCypherPath) {
                path = create(
                    transaction,
                    relationshipIdOffset,
                    nodeIds,
                    costs,
                    relationshipType,
                    COST_PROPERTY_NAME
                );
                relationshipIdOffset -= path.length();
            }

            // 😿
            var nodeIdsList = new ArrayList<Long>(nodeIds.length);
            for (long nodeId : nodeIds) {
                nodeIdsList.add(idMapping.toOriginalNodeId(nodeId));
            }
            var costsList = new ArrayList<Double>(costs.length);
            for (double cost : costs) {
                costsList.add(cost);
            }

            return new StreamResult(
                pathIndex,
                idMapping.toOriginalNodeId(pathResult.sourceNode()),
                idMapping.toOriginalNodeId(pathResult.targetNode()),
                pathResult.totalCost(),
                nodeIdsList,
                costsList,
                path
            );
        }
    }
}
