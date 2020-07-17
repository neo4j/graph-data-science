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
package org.neo4j.graphalgo.pregel.cc;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.examples.ConnectedComponentsPregel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.logging.Log;

// generate
public class ConnectedComponentsAlgorithm extends Algorithm<ConnectedComponentsAlgorithm, HugeDoubleArray> {

    private final Pregel pregelJob;
    private final int maxIterations;

    ConnectedComponentsAlgorithm(
        Graph graph,
        ConnectedComponentsConfig configuration,
        AllocationTracker tracker,
        Log log
    ) {
        this.maxIterations = configuration.maxIterations();

        var concurrency = configuration.concurrency();
        var batchSize = (int) ParallelUtil.adjustedBatchSize(graph.nodeCount(), concurrency);

        pregelJob = Pregel.withDefaultNodeValues(
            graph,
            configuration,
            new ConnectedComponentsPregel(),
            batchSize,
            Pools.DEFAULT,
            tracker
        );
    }

    @Override
    public HugeDoubleArray compute() {
        return pregelJob.run(maxIterations);
    }

    @Override
    public ConnectedComponentsAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
    }
}
