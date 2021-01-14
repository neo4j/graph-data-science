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
package org.neo4j.graphalgo.test;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

public class TestAlgorithm extends Algorithm<TestAlgorithm, TestAlgorithm> {

    private final Graph graph;
    private final AllocationTracker allocationTracker;
    private long relationshipCount = 0;
    private final long memoryLimit;

    public TestAlgorithm(Graph graph, AllocationTracker allocationTracker, long memoryLimit) {
        this.graph = graph;
        this.allocationTracker = allocationTracker;
        this.memoryLimit = memoryLimit;
    }

    @Override
    public TestAlgorithm compute() {
        relationshipCount = graph.relationshipCount();
        allocationTracker.add(memoryLimit * 2);
        return this;
    }

    @Override
    public TestAlgorithm me() {
        return this;
    }

    @Override
    public void release() {}

    long relationshipCount() {
        return relationshipCount;
    }
}
