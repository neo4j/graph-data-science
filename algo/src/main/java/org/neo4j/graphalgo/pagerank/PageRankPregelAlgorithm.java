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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.concurrent.ExecutorService;

public class PageRankPregelAlgorithm extends Algorithm<PageRankPregelAlgorithm, PageRankPregelResult> {

    private final Pregel<PageRankPregelConfig> pregelJob;

    PageRankPregelAlgorithm(
        Graph graph,
        PageRankPregelConfig config,
        PregelComputation<PageRankPregelConfig> pregelComputation,
        ExecutorService executorService,
        AllocationTracker tracker
    ) {
        this.pregelJob = Pregel.create(graph, config, pregelComputation, executorService, tracker);
    }

    @Override
    public PageRankPregelResult compute() {
        var pregelResult = pregelJob.run();

        return ImmutablePageRankPregelResult.builder()
            .scores(pregelResult.nodeValues().doubleProperties(PageRankPregel.PAGE_RANK))
            .iterations(pregelResult.ranIterations())
            .didConverge(pregelResult.didConverge())
            .build();
    }

    @Override
    public PageRankPregelAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
        this.pregelJob.release();
    }
}
