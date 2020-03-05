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
package gds.training;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

public class K1ColoringAlgorithm extends Algorithm<K1ColoringAlgorithm, HugeDoubleArray> {

    private final Graph graph;
    private final int maxIterations;

    K1ColoringAlgorithm(Graph graph, int maxIterations) {
        this.graph = graph;
        this.maxIterations = maxIterations;
    }

    @Override
    public HugeDoubleArray compute() {
        PregelConfig config = new PregelConfig.Builder()
                .isAsynchronous(true)
                .build();

        Pregel pregelJob = Pregel.withDefaultNodeValues(
                graph,
                config,
                new K1ColoringExample(),
                10,
                AlgoBaseConfig.DEFAULT_CONCURRENCY,
                Pools.DEFAULT,
                AllocationTracker.EMPTY
        );

        return pregelJob.run(maxIterations);
    }

    @Override
    public K1ColoringAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
        graph.release();
    }
}
