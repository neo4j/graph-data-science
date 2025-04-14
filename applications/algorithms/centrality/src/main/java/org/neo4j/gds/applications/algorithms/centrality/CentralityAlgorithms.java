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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.articulationpoints.ArticulationPoints;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeProgressTaskCreator;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.Bridges;
import org.neo4j.gds.closeness.ClosenessCentrality;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityTask;
import org.neo4j.gds.closeness.DefaultCentralityComputer;
import org.neo4j.gds.closeness.WassermanFaustCentralityComputer;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentrality;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.Hits;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.hits.HitsProgressTrackerCreator;
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
import org.neo4j.gds.termination.TerminationFlag;

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
        var progressTracker = createProgressTracker(task, configuration);

        return articleRank(graph, configuration, progressTracker);
    }

    public PageRankResult articleRank(Graph graph, ArticleRankConfig configuration, ProgressTracker progressTracker) {
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

    ArticulationPointsResult articulationPoints(
        Graph graph,
        ArticulationPointsParameters parameters,
        ProgressTracker progressTracker)
    {

        var algorithm =  ArticulationPoints.create(graph, parameters, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }



    public BetwennessCentralityResult betweennessCentrality(
        Graph graph,
        BetweennessCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {

        var algorithm = BetweennessCentrality.create(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            parameters.concurrency()
        );
    }

    BridgeResult bridges(Graph graph, AlgoBaseConfig configuration, boolean shouldComputeComponents) {

        var task = BridgeProgressTaskCreator.progressTask(graph.nodeCount());
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = Bridges.create(graph, progressTracker, shouldComputeComponents);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public CELFResult celf(Graph graph, InfluenceMaximizationBaseConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.CELF.asString(),
            Tasks.leaf("Greedy", graph.nodeCount()),
            Tasks.leaf("LazyForwarding", configuration.seedSetSize() - 1)
        );
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new CELF(graph, configuration.toParameters(), DefaultPool.INSTANCE, progressTracker);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    ClosenessCentralityResult closenessCentrality(Graph graph, ClosenessCentralityBaseConfig configuration) {
        var task = ClosenessCentralityTask.create(graph.nodeCount());
        var progressTracker = createProgressTracker(task, configuration);

        return closenessCentrality(graph, configuration, progressTracker);
    }

    public ClosenessCentralityResult closenessCentrality(
        Graph graph,
        ClosenessCentralityBaseConfig configuration,
        ProgressTracker progressTracker
    ) {
        var parameters = configuration.toParameters();

        var centralityComputer = parameters.useWassermanFaust()
            ? new WassermanFaustCentralityComputer(graph.nodeCount())
            : new DefaultCentralityComputer();

        var algorithm = new ClosenessCentrality(
            graph,
            parameters.concurrency(),
            centralityComputer,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    DegreeCentralityResult degreeCentrality(Graph graph, DegreeCentralityConfig configuration) {
        var parameters = configuration.toParameters();

        var task = Tasks.leaf(AlgorithmLabel.DegreeCentrality.asString(), graph.nodeCount());
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new DegreeCentrality(
            graph,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            parameters.orientation(),
            parameters.hasRelationshipWeightProperty(),
            parameters.minBatchSize(),
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    PageRankResult eigenVector(Graph graph, EigenvectorConfig configuration) {
        var task = Pregel.progressTask(graph, configuration, EigenVector.asString());
        var progressTracker = createProgressTracker(task, configuration);

        return eigenVector(graph, configuration, progressTracker);
    }

    public PageRankResult eigenVector(
        Graph graph,
        EigenvectorConfig configuration,
        ProgressTracker progressTracker
    ) {
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

    HarmonicResult harmonicCentrality(Graph graph, AlgoBaseConfig configuration) {
        var task = Tasks.leaf(AlgorithmLabel.HarmonicCentrality.asString());
        var progressTracker = createProgressTracker(task, configuration);

        return harmonicCentrality(graph, configuration, progressTracker);
    }

    public HarmonicResult harmonicCentrality(
        Graph graph,
        ConcurrencyConfig configuration,
        ProgressTracker progressTracker
    ) {
        var algorithm = new HarmonicCentrality(
            graph,
            configuration.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    PregelResult hits(Graph graph, HitsConfig configuration) {
        var task = HitsProgressTrackerCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations(),
            AlgorithmLabel.HITS.asString()
        );
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new Hits(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    IndirectExposureResult indirectExposure(Graph graph, IndirectExposureConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.IndirectExposure.asString(),
            Tasks.leaf("TotalTransfers", graph.nodeCount()),
            Pregel.progressTask(graph, configuration, "ExposurePropagation")
        );
        var progressTracker = createProgressTracker(task, configuration);

        var algorithm = new IndirectExposure(
            graph,
            configuration,
            DefaultPool.INSTANCE,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(
            algorithm,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }

    public PageRankResult pageRank(Graph graph, PageRankConfig configuration) {
        var task = Pregel.progressTask(graph, configuration, PageRank.asString());
        var progressTracker = createProgressTracker(task, configuration);

        return pageRank(graph, configuration, progressTracker);
    }

    public PageRankResult pageRank(Graph graph, PageRankConfig configuration, ProgressTracker progressTracker) {
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

    private ArticleRankComputation<ArticleRankConfig> articleRankComputation(
        Graph graph,
        ArticleRankConfig configuration
    ) {
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

        return new ArticleRankComputation<>(configuration, mappedSourceNodes, degreeFunction, avgDegree);
    }

    private EigenvectorComputation<EigenvectorConfig> eigenvectorComputation(
        Graph graph,
        EigenvectorConfig configuration
    ) {
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

        return new EigenvectorComputation<>(
            graph.nodeCount(),
            configuration,
            mappedSourceNodes,
            degreeFunction
        );
    }

    private PageRankComputation<PageRankConfig> pageRankComputation(Graph graph, PageRankConfig configuration) {
        var degreeFunction = DegreeFunctions.pageRankDegreeFunction(
            graph,
            configuration.hasRelationshipWeightProperty(), configuration.concurrency()
        );

        var mappedSourceNodes = new LongScatterSet(configuration.sourceNodes().size());
        configuration.sourceNodes().stream()
            .mapToLong(graph::toMappedNodeId)
            .forEach(mappedSourceNodes::add);

        return new PageRankComputation<>(configuration, mappedSourceNodes, degreeFunction);
    }

    private ProgressTracker createProgressTracker(Task task, AlgoBaseConfig configuration) {
        return progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );
    }
}
