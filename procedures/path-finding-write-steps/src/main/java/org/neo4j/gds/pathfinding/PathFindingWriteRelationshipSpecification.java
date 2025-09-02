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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;

public final class PathFindingWriteRelationshipSpecification {

    private static final String TOTAL_COST_KEY = "totalCost";
    private static final String NODE_IDS_KEY = "nodeIds";
    private static final String COSTS_KEY = "costs";


    private final IdMap idMap;
    private final boolean writeNodeIds;
    private final boolean writeCosts;

    public PathFindingWriteRelationshipSpecification(IdMap idMap, boolean writeNodeIds, boolean writeCosts) {
        this.idMap = idMap;
        this.writeNodeIds = writeNodeIds;
        this.writeCosts = writeCosts;
    }

    public List<String> createKeys() {
        if (writeNodeIds && writeCosts) {
            return List.of(
                TOTAL_COST_KEY,
                NODE_IDS_KEY,
                COSTS_KEY
            );
        }
        if (writeNodeIds) {
            return List.of(
                TOTAL_COST_KEY,
                NODE_IDS_KEY
            );
        }
        if (writeCosts) {
            return List.of(
                TOTAL_COST_KEY,
                COSTS_KEY
            );
        }
        return List.of(TOTAL_COST_KEY);
    }

     public List<ValueType> createTypes() {
        if (writeNodeIds && writeCosts) {
            return List.of(
                ValueType.DOUBLE,
                ValueType.LONG_ARRAY,
                ValueType.DOUBLE_ARRAY
            );
        }
        if (writeNodeIds) {
            return List.of(
                ValueType.DOUBLE,
                ValueType.LONG_ARRAY
            );
        }
        if (writeCosts) {
            return List.of(
                ValueType.DOUBLE,
                ValueType.DOUBLE_ARRAY
            );
        }
        return List.of(ValueType.DOUBLE);
    }

    private  Value[] createValues(PathResult pathResult) {
        if (writeNodeIds && writeCosts) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.longArray(toOriginalIds(pathResult.nodeIds())),
                Values.doubleArray(pathResult.costs())
            };
        }
        if (writeNodeIds) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.longArray(toOriginalIds(pathResult.nodeIds())),
            };
        }
        if (writeCosts) {
            return new Value[]{
                Values.doubleValue(pathResult.totalCost()),
                Values.doubleArray(pathResult.costs())
            };
        }
        return new Value[]{
            Values.doubleValue(pathResult.totalCost()),
        };
    }

    // Replaces the ids in the given array with the original ids
    private long[] toOriginalIds( long[] internalIds) {
        for (int i = 0; i < internalIds.length; i++) {
            internalIds[i] = idMap.toOriginalNodeId(internalIds[i]);
        }
        return internalIds;
    }

     public ExportedRelationship createRelationship(PathResult pathResult){
        return new ExportedRelationship(
            pathResult.sourceNode(),
            pathResult.targetNode(),
            createValues(pathResult)
        );
    }

}
