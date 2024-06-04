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
package org.neo4j.gds.applications.algorithms.centrality;

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.betweenness.ForwardTraverser;
import org.neo4j.gds.betweenness.FullSelectionStrategy;
import org.neo4j.gds.betweenness.RandomDegreeSelectionStrategy;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.closeness.DefaultCentralityComputer;
import org.neo4j.gds.closeness.WassermanFaustCentralityComputer;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentrality;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.influenceMaximization.CELF;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.ArticleRankComputation;
import org.neo4j.gds.pagerank.EigenvectorComputation;
import org.neo4j.gds.pagerank.PageRankAlgorithm;
import org.neo4j.gds.pagerank.PageRankAlgorithmFactory;
import org.neo4j.gds.pagerank.PageRankComputation;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.gds.pagerank.PageRankAlgorithmFactory.Mode.ARTICLE_RANK;
import static org.neo4j.gds.pagerank.PageRankAlgorithmFactory.Mode.EIGENVECTOR;
import static org.neo4j.gds.pagerank.PageRankAlgorithmFactory.Mode.PAGE_RANK;

public class CentralityAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public CentralityAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    PageRankResult articleRank(Graph graph, PageRankConfig configuration) {
        return pagerank(graph, configuration, LabelForProgressTracking.ArticleRank, ARTICLE_RANK);
    }

    BetwennessCentralityResult betweennessCentrality(Graph graph, BetweennessCentralityBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var samplingSize = parameters.samplingSize();
        var samplingSeed = parameters.samplingSeed();

        var selectionStrategy = samplingSize.isPresent() && samplingSize.get() < graph.nodeCount()
            ? new RandomDegreeSelectionStrategy(samplingSize.get(), samplingSeed)
            : new FullSelectionStrategy();

        var traverserFactory = parameters.hasRelationshipWeightProperty()
            ? ForwardTraverser.Factory.weighted()
            : ForwardTraverser.Factory.unweighted();

        var task = Tasks.leaf(
            LabelForProgressTracking.BetweennessCentrality.value,
            samplingSize.orElse(graph.nodeCount())
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new BetweennessCentrality(
            graph,
            selectionStrategy,
            traverserFactory,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    CELFResult celf(Graph graph, InfluenceMaximizationBaseConfig configuration) {
        var task = Tasks.task(
            LabelForProgressTracking.CELF.value,
            Tasks.leaf("Greedy", graph.nodeCount()),
            Tasks.leaf("LazyForwarding", configuration.seedSetSize() - 1)
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new CELF(graph, configuration.toParameters(), DefaultPool.INSTANCE, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    ClosenessCentralityResult closenessCentrality(Graph graph, ClosenessCentralityBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var centralityComputer = parameters.useWassermanFaust()
            ? new WassermanFaustCentralityComputer(graph.nodeCount())
            : new DefaultCentralityComputer();

        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, Tasks.task(
            LabelForProgressTracking.ClosenessCentrality.value,
            Tasks.leaf("Farness computation", graph.nodeCount() * graph.nodeCount()),
            Tasks.leaf("Closeness computation", graph.nodeCount())
        ));

        var algorithm = new ClosenessCentrality(
            graph,
            parameters.concurrency(),
            centralityComputer,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    DegreeCentralityResult degreeCentrality(Graph graph, DegreeCentralityConfig configuration) {
        var parameters = configuration.toParameters();

        var task = Tasks.leaf(LabelForProgressTracking.DegreeCentrality.value, graph.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.orientation(),
            parameters.hasRelationshipWeightProperty(),
            parameters.minBatchSize(),
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    PageRankResult eigenVector(Graph graph, PageRankConfig configuration) {
        return pagerank(graph, configuration, LabelForProgressTracking.EigenVector, EIGENVECTOR);
    }

    HarmonicResult harmonicCentrality(Graph graph, HarmonicCentralityBaseConfig configuration) {
        var task = Tasks.leaf(LabelForProgressTracking.HarmonicCentrality.value);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new HarmonicCentrality(
            graph,
            configuration.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    PageRankResult pageRank(Graph graph, PageRankConfig configuration) {
        return pagerank(graph, configuration, LabelForProgressTracking.PageRank, PAGE_RANK);
    }

    private double averageDegree(Graph graph, Concurrency concurrency) {
        var degreeSum = new LongAdder();
        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            nodeId -> degreeSum.add(graph.degree(nodeId))
        );
        return (double) degreeSum.sum() / graph.nodeCount();
    }

    private LongToDoubleFunction degreeFunction(
        Graph graph,
        PageRankConfig configuration
    ) {
        var degreeCentrality = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            Orientation.NATURAL,
            configuration.hasRelationshipWeightProperty(),
            10_000,
            ProgressTracker.NULL_TRACKER
        );

        var degrees = degreeCentrality.compute().degreeFunction();
        return degrees::get;
    }

    private PageRankResult pagerank(
        Graph graph,
        PageRankConfig configuration,
        LabelForProgressTracking label,
        PageRankAlgorithmFactory.Mode mode
    ) {
        var task = Pregel.progressTask(graph, configuration, label.value);
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var computation = pickComputation(graph, configuration, mode);

        var algorithm = new PageRankAlgorithm(
            graph,
            configuration,
            computation,
            mode,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    private PregelComputation<PageRankConfig> pickComputation(
        Graph graph, PageRankConfig configuration,
        PageRankAlgorithmFactory.Mode mode
    ) {
        var degreeFunction = degreeFunction(
            graph,
            configuration
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());

        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        if (mode == ARTICLE_RANK) {
            double avgDegree = averageDegree(graph, configuration.concurrency());
            return new ArticleRankComputation(configuration, mappedSourceNodes, degreeFunction, avgDegree);
        } else if (mode == EIGENVECTOR) {
            // Degrees are generally not respected in eigenvector centrality.
            //
            // However, relationship weights need to be normalized by the weighted degree.
            // The score is divided by the weighted degree before being sent to the neighbors.
            // For the unweighted case, we want a no-op and divide by 1.
            degreeFunction = configuration.hasRelationshipWeightProperty()
                ? degreeFunction
                : (nodeId) -> 1;

            return new EigenvectorComputation(graph.nodeCount(), configuration, mappedSourceNodes, degreeFunction);
        } else {
            return new PageRankComputation(configuration, mappedSourceNodes, degreeFunction);
        }
    }
}
