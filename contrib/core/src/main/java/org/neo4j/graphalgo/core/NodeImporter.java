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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class NodeImporter extends BaseNodeImporter<IdMap> {

    private final AllocationTracker tracker;

    public NodeImporter(
            GraphDatabaseAPI api,
            AllocationTracker tracker,
            ImportProgress progress,
            int nodeCount,
            int labelId) {
        super(api, progress, nodeCount, labelId);
        this.tracker = tracker;
    }

    @Override
    protected IdMap newNodeMap(final long nodeCount) {
        return new IdMap((int) nodeCount);
    }

    @Override
    protected void addNodeId(final IdMap map, final long nodeId) {
        map.add(nodeId);
    }

    @Override
    protected void finish(final IdMap map) {
        map.buildMappedIds(tracker);
    }
}
