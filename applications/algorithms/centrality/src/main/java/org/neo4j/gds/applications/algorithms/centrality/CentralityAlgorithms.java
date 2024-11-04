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

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.articulationpoints.ArticulationPoints;
import org.neo4j.gds.articulationpoints.ArticulationPointsProgressTaskCreator;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.betweenness.ForwardTraverser;
import org.neo4j.gds.betweenness.FullSelectionStrategy;
import org.neo4j.gds.betweenness.RandomDegreeSelectionStrategy;
import org.neo4j.gds.bridges.BridgeProgressTaskCreator;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.closeness.DefaultCentralityComputer;
import org.neo4j.gds.closeness.WassermanFaustCentralityComputer;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentrality;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.indirectExposure.IndirectExposure;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELF;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.ArticleRankComputation;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.DegreeFunctions;
import org.neo4j.gds.pagerank.EigenvectorComputation;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankAlgorithm;
import org.neo4j.gds.pagerank.PageRankComputation;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.sllpa.SpeakerListenerLPA;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAProgressTrackerCreator;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;
import static org.neo4j.gds.pagerank.PageRankVariant.ARTICLE_RANK;
import static org.neo4j.gds.pagerank.PageRankVariant.EIGENVECTOR;
import static org.neo4j.gds.pagerank.PageRankVariant.PAGE_RANK;

public class CentralityAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public CentralityAlgorithms(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    PageRankResult articleRank(Graph graph, ArticleRankConfig configuration) {
        var task = Pregel.progressTask(graph, configuration, ArticleRank.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var articleRankComputation = articleRankComputation(graph, configuration);

        var articleRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            articleRankComputation,
            ARTICLE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return articleRank.compute();
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
            AlgorithmLabel.BetweennessCentrality.asString(),
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
            AlgorithmLabel.CELF.asString(),
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
            AlgorithmLabel.ClosenessCentrality.asString(),
            Tasks.leaf("Farness computation", graph.nodeCount() * graph.nodeCount()),
            Tasks.leaf("Closeness computation", graph.nodeCount())
        ));

        var algorithm = new ClosenessCentrality(
            graph,
            parameters.concurrency(),
            centralityComputer,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    IndirectExposureResult indirectExposure(Graph graph, IndirectExposureConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.IndirectExposure.asString(),
            Tasks.leaf("TotalTransfers", graph.nodeCount()),
            Pregel.progressTask(graph, configuration, "ExposurePropagation")
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new IndirectExposure(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    DegreeCentralityResult degreeCentrality(Graph graph, DegreeCentralityConfig configuration) {
        var parameters = configuration.toParameters();

        var task = Tasks.leaf(AlgorithmLabel.DegreeCentrality.asString(), graph.nodeCount());
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

    BitSet articulationPoints(Graph graph, AlgoBaseConfig configuration) {

        var task = ArticulationPointsProgressTaskCreator.progressTask(graph.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new ArticulationPoints(graph, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    BridgeResult bridges(Graph graph, AlgoBaseConfig configuration) {

        var task = BridgeProgressTaskCreator.progressTask(graph.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new Bridges(graph, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    PageRankResult eigenVector(Graph graph, EigenvectorConfig configuration) {
        var task = Pregel.progressTask(graph, configuration, EigenVector.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var eigenvectorComputation = eigenvectorComputation(graph, configuration);

        var eigenvector = new PageRankAlgorithm<>(
            graph,
            configuration,
            eigenvectorComputation,
            EIGENVECTOR,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return eigenvector.compute();
    }

    HarmonicResult harmonicCentrality(Graph graph, HarmonicCentralityBaseConfig configuration) {
        var task = Tasks.leaf(AlgorithmLabel.HarmonicCentrality.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new HarmonicCentrality(
            graph,
            configuration.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    PageRankResult pageRank(Graph graph, PageRankConfig configuration) {
        var task = Pregel.progressTask(graph, configuration, PageRank.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var pageRankComputation = pageRankComputation(graph, configuration);
        var pageRank = new PageRankAlgorithm<>(
            graph,
            configuration,
            pageRankComputation,
            PAGE_RANK,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );
        return pageRank.compute();
    }

    private ArticleRankComputation articleRankComputation(Graph graph, ArticleRankConfig configuration) {
        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(),
            configuration.concurrency()
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        double avgDegree = DegreeFunctions.averageDegree(graph, configuration.concurrency());

        return new ArticleRankComputation(configuration, mappedSourceNodes, degreeFunction, avgDegree);
    }

    private EigenvectorComputation eigenvectorComputation(Graph graph, EigenvectorConfig configuration) {
        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        boolean hasRelationshipWeightProperty = configuration.hasRelationshipWeightProperty();
        Concurrency concurrency = configuration.concurrency();
        var degreeFunction = DegreeFunctions.eigenvectorDegreeFunction(
            graph,
            hasRelationshipWeightProperty,
            concurrency
        );

        return new EigenvectorComputation(
            graph.nodeCount(),
            configuration,
            mappedSourceNodes,
            degreeFunction
        );
    }

    private PageRankComputation pageRankComputation(Graph graph, PageRankConfig configuration) {
        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(), configuration.concurrency()
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        return new PageRankComputation(configuration, mappedSourceNodes, degreeFunction);
    }

    PregelResult speakerListenerLPA(Graph graph, SpeakerListenerLPAConfig configuration){
        var task  =SpeakerListenerLPAProgressTrackerCreator.progressTask(graph.nodeCount(),configuration.maxIterations(),AlgorithmLabel.SLLPA.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new SpeakerListenerLPA(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker,
            Optional.empty()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }
}
