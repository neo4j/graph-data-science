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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;

public class BasicDegreeComputer implements DegreeComputer {
    private final Graph graph;

    BasicDegreeComputer(Graph graph) {
        this.graph = graph;
    }

    @Override
    public DegreeCache degree(
            ExecutorService executor,
            int concurrency,
            AllocationTracker tracker) {
        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(graph, executor, concurrency);
        degreeCentrality.compute();
        return DegreeCache.EMPTY.withAverage(degreeCentrality.average());
    }
}
