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
package org.neo4j.graphalgo.impl.results;

import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphdb.Result;

import java.util.Map;

public class MemoryEstimateResult {
    public final String requiredMemory;
    public final String treeView;
    public final Map<String, Object> mapView;
    public final long bytesMin, bytesMax;
    public long nodeCount, relationshipCount;

    public MemoryEstimateResult(MemoryTreeWithDimensions memory) {
        this(memory.memoryTree, memory.graphDimensions);
    }

    private MemoryEstimateResult(MemoryTree memory, GraphDimensions dimensions) {
        this(memory.render(), memory.renderMap(), memory.memoryUsage(), dimensions);
    }

    private MemoryEstimateResult(
        String treeView,
        Map<String, Object> mapView,
        MemoryRange estimateMemoryUsage,
        GraphDimensions dimensions
    ) {
        this(
            estimateMemoryUsage.toString(),
            treeView,
            mapView,
            estimateMemoryUsage.min,
            estimateMemoryUsage.max,
            dimensions.nodeCount(),
            dimensions.maxRelCount()
        );
    }

    private MemoryEstimateResult(
        String requiredMemory,
        String treeView,
        Map<String, Object> mapView,
        long bytesMin,
        long bytesMax,
        long nodeCount,
        long relationshipCount
    ) {
        this.requiredMemory = requiredMemory;
        this.treeView = treeView;
        this.mapView = mapView;
        this.bytesMin = bytesMin;
        this.bytesMax = bytesMax;
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
    }

    public MemoryEstimateResult(Result.ResultRow row) {
        this(
            row.getString("requiredMemory"),
            row.getString("treeView"),
            (Map<String, Object>) row.get("mapView"),
            row.getNumber("bytesMin").longValue(),
            row.getNumber("bytesMax").longValue(),
            row.getNumber("nodeCount").longValue(),
            row.getNumber("relationshipCount").longValue()
        );
    }
}
