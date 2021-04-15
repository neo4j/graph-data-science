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

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStatistics;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.logging.Log;

import java.util.function.LongToDoubleFunction;

public class PageRankPregelAlgorithmFactory<CONFIG extends PageRankPregelConfig> implements AlgorithmFactory<PageRankPregelAlgorithm, CONFIG> {

    @Override
    public PageRankPregelAlgorithm build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        LongToDoubleFunction degreeFunction;
        double deltaCoefficient = 1;

        if (configuration.isWeighted()) {
            var aggregatedWeights = new WeightedDegreeComputer(graph)
                .degree(Pools.DEFAULT, configuration.concurrency(), tracker)
                .aggregatedDegrees();
            degreeFunction = aggregatedWeights::get;
        } else {
            degreeFunction = graph::degree;
        }

        if (configuration.mode() == PageRankPregelConfig.Mode.ARTICLE_RANK) {
            double avgDegree = GraphStatistics.averageDegree(graph, configuration.concurrency());
            var tempFn = degreeFunction;
            degreeFunction = nodeId -> tempFn.applyAsDouble(nodeId) + avgDegree;
            deltaCoefficient = avgDegree;
        }

        var computation = new PageRankPregel(graph, configuration, degreeFunction, deltaCoefficient);

        return new PageRankPregelAlgorithm(graph, configuration, computation, Pools.DEFAULT, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(PageRankPregelConfig configuration) {
        return Pregel.memoryEstimation(new PregelSchema.Builder()
            .add(PageRankPregel.PAGE_RANK, ValueType.DOUBLE)
            .build(), false, false);
    }
}
