/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphdb.Result;

import java.util.Map;

public class MemRecResult {
    public final String requiredMemory;
    public final String treeView;
    public final Map<String, Object> mapView;
    public final long bytesMin, bytesMax;

    public MemRecResult(final MemoryTree memoryRequirements) {
        this(memoryRequirements.render(), memoryRequirements.renderMap(), memoryRequirements.memoryUsage());
    }

    private MemRecResult(
            final String treeView,
            final Map<String, Object> mapView,
            final MemoryRange estimateMemoryUsage) {
        this(estimateMemoryUsage.toString(), treeView, mapView, estimateMemoryUsage.min, estimateMemoryUsage.max);
    }

    private MemRecResult(
            final String requiredMemory,
            final String treeView,
            final Map<String, Object> mapView,
            final long bytesMin,
            final long bytesMax) {
        this.requiredMemory = requiredMemory;
        this.treeView = treeView;
        this.mapView = mapView;
        this.bytesMin = bytesMin;
        this.bytesMax = bytesMax;
    }

    public MemRecResult(final Result.ResultRow row) {
        this(
                row.getString("requiredMemory"),
                row.getString("treeView"),
                (Map<String, Object>) row.get("mapView"),
                row.getNumber("bytesMin").longValue(),
                row.getNumber("bytesMax").longValue()
        );
    }
}
