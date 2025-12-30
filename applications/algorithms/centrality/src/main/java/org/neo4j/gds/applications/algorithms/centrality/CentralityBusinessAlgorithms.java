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

import org.neo4j.gds.CentralityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsToParameters;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.bridges.BridgesBaseConfig;
import org.neo4j.gds.bridges.BridgesToParameters;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityBaseConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationBaseConfig;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public class CentralityBusinessAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();
    private final CentralityAlgorithms centralityAlgorithms;
    private final ProgressTrackerCreator progressTrackerCreator;
    private final CentralityAlgorithmTasks tasks = new CentralityAlgorithmTasks();

    public CentralityBusinessAlgorithms(
        CentralityAlgorithms centralityAlgorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.centralityAlgorithms = centralityAlgorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    PageRankResult articleRank(Graph graph, ArticleRankConfig configuration) {
        var task = tasks.articleRank(graph,configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.articleRank(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }


    ArticulationPointsResult articulationPoints(
        Graph graph,
        ArticulationPointsBaseConfig configuration,
        boolean shouldComputeComponents
    ) {
        var task = tasks.articulationPoints(graph);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        var params = ArticulationPointsToParameters.toParameters(configuration, shouldComputeComponents);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.articulationPoints(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    BetwennessCentralityResult betweennessCentrality(Graph graph, BetweennessCentralityBaseConfig configuration) {
        var params = configuration.toParameters();
        var task = tasks.betweennessCentrality(graph, params);

        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.betweennessCentrality(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    BridgeResult bridges(Graph graph, BridgesBaseConfig configuration, boolean shouldComputeComponents) {

        var task = tasks.bridges(graph);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        var params = BridgesToParameters.toParameters(configuration, shouldComputeComponents);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.bridges(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    public CELFResult celf(Graph graph, InfluenceMaximizationBaseConfig configuration) {

        var params = configuration.toParameters();
        var task = tasks.CELF(graph, params);

        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.celf(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }


    public ClosenessCentralityResult closenessCentrality(
        Graph graph,
        ClosenessCentralityBaseConfig configuration
    ) {

        var params = configuration.toParameters();
        var task = tasks.closenessCentrality(graph);

        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.closenessCentrality(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    DegreeCentralityResult degreeCentrality(Graph graph, DegreeCentralityConfig configuration) {

        var params = configuration.toParameters();
        var task = tasks.degreeCentrality(graph);

        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.degreeCentrality(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    PageRankResult eigenVector(Graph graph, EigenvectorConfig configuration) {

        var task = tasks.eigenVector(graph,configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.eigenVector(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );

    }

    HarmonicResult harmonicCentrality(Graph graph, HarmonicCentralityBaseConfig configuration) {
        var params = configuration.toParameters();
        var task = tasks.harmonicCentrality();

        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.harmonicCentrality(graph, params, progressTracker),
            progressTracker,
            params.concurrency()
        );
    }

    PregelResult hits(Graph graph, HitsConfig configuration) {

        var task = tasks.hits(graph,configuration);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.hits(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }

    IndirectExposureResult indirectExposure(Graph graph, IndirectExposureConfig configuration) {
        var task = Tasks.task(
            AlgorithmLabel.IndirectExposure.asString(),
            Tasks.leaf("TotalTransfers", graph.nodeCount()),
            Pregel.progressTask(graph, configuration, "ExposurePropagation")
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.indirectExposure(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }

    public PageRankResult pageRank(Graph graph, PageRankConfig configuration) {

        var task = Pregel.progressTask(graph, configuration, PageRank.asString());
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> centralityAlgorithms.pageRank(graph, configuration, progressTracker),
            progressTracker,
            configuration.concurrency()
        );
    }

}
