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
package org.neo4j.gds.topologicalsort;

import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of the result with message queues instead of synchronization.
 */
public class TopologicalSortResult {
    private HugeLongArray sortedNodes;
    private AtomicLong addIndex = new AtomicLong(0);

    TopologicalSortResult(long nodeCount) {
        sortedNodes = HugeLongArray.newArray(nodeCount);
    }

    public HugeLongArray value() {
        return sortedNodes;
    }
    public long size() {
        return addIndex.get();
    }

    void addNode(long nodeId) {
        sortedNodes.set(addIndex.getAndIncrement(), nodeId);
    }
}
