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

import com.carrotsearch.hppc.LongScatterSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.graphalgo.degree.DegreeCentrality;
import org.neo4j.graphalgo.degree.DegreeCentralityFactory;
import org.neo4j.graphalgo.degree.ImmutableDegreeCentralityConfig;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.graphalgo.pagerank.PageRankAlgorithmFactory.Mode.ARTICLE_RANK;
import static org.neo4j.graphalgo.pagerank.PageRankAlgorithmFactory.Mode.EIGENVECTOR;

public class PageRankAlgorithmFactory<CONFIG extends PageRankConfig> extends AbstractAlgorithmFactory<PageRankAlgorithm, CONFIG> {

    static Task pagerankProgressTask(Graph graph) {
        return Tasks.task(
            "PageRank",
            DegreeCentralityFactory.degreeCentralityProgressTask(graph)
        );
    }

    private static double averageDegree(Graph graph, int concurrency) {
        var degreeSum = new LongAdder();
        ParallelUtil.parallelForEachNode(
            graph,
            concurrency,
            nodeId -> degreeSum.add(graph.degree(nodeId))
        );
        return (double) degreeSum.sum() / graph.nodeCount();
    }

    public enum Mode {
        PAGE_RANK,
        ARTICLE_RANK,
        EIGENVECTOR,
    }

    private final Mode mode;

    public PageRankAlgorithmFactory() {
        this(Mode.PAGE_RANK);
    }

    public PageRankAlgorithmFactory(Mode mode) {
        this.mode = mode;
    }

    @Override
    protected long taskVolume(Graph graph, PageRankConfig configuration) {
        return graph.nodeCount();
    }

    @Override
    protected String taskName() {
        return mode.name();
    }

    @Override
    protected PageRankAlgorithm build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        PregelComputation<PageRankConfig> computation;
        double deltaCoefficient = 1;

        var degreeFunction = degreeFunction(
            graph,
            configuration,
            tracker,
            progressTracker
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        if (mode == ARTICLE_RANK) {
            double avgDegree = averageDegree(graph, configuration.concurrency());
            var tempFn = degreeFunction;
            degreeFunction = nodeId -> tempFn.applyAsDouble(nodeId) + avgDegree;
            deltaCoefficient = avgDegree;
            computation = new PageRankComputation(configuration, mappedSourceNodes, degreeFunction, deltaCoefficient);
        } else if (mode == EIGENVECTOR) {
            // Degrees are generally not respected in eigenvector centrality.
            //
            // However, relationship weights need to be normalized by the weighted degree.
            // The score is divided by the weighted degree before being sent to the neighbors.
            // For the unweighted case, we want a no-op and divide by 1.
            degreeFunction = configuration.hasRelationshipWeightProperty()
                ? degreeFunction
                : (nodeId) -> 1;

            computation = new EigenvectorComputation(graph.nodeCount(), configuration, mappedSourceNodes, degreeFunction);
        } else {
            computation = new PageRankComputation(configuration, mappedSourceNodes, degreeFunction, deltaCoefficient);
        }

        return new PageRankAlgorithm(graph, configuration, computation, mode, Pools.DEFAULT, tracker, progressTracker);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return pagerankProgressTask(graph);
    }

    @NotNull
    private LongToDoubleFunction degreeFunction(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        progressTracker.beginSubTask();
        var config = ImmutableDegreeCentralityConfig.builder()
            .concurrency(configuration.concurrency())
            .relationshipWeightProperty(configuration.relationshipWeightProperty())
            .build();

        var degreeCentrality = new DegreeCentrality(
            graph,
            Pools.DEFAULT,
            config,
            progressTracker,
            tracker
        );

        var degrees = degreeCentrality.compute();
        progressTracker.endSubTask();
        return degrees::get;
    }

    @Override
    public MemoryEstimation memoryEstimation(PageRankConfig configuration) {
        return Pregel.memoryEstimation(new PregelSchema.Builder()
            .add(PageRankComputation.PAGE_RANK, ValueType.DOUBLE)
            .build(), false, false);
    }
}
