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
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.articulationpoints.ArticulationPointsBaseConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsToParameters;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
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

public class CentralityBusinessAlgorithms {

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
        return centralityAlgorithms.articleRank(graph, configuration);
    }

    public PageRankResult articleRank(Graph graph, ArticleRankConfig configuration, ProgressTracker progressTracker) {

        return centralityAlgorithms.articleRank(graph, configuration, progressTracker);
    }

    ArticulationPointsResult articulationPoints(
        Graph graph,
        ArticulationPointsBaseConfig configuration,
        boolean shouldComputeComponents
    ) {
        var task = tasks.articulationPoints(graph);
        var progressTracker =  progressTrackerCreator.createProgressTracker(task,configuration);
        var params = ArticulationPointsToParameters.toParameters(configuration, shouldComputeComponents);
        return centralityAlgorithms.articulationPoints(graph, params, progressTracker);
    }

    BetwennessCentralityResult betweennessCentrality(Graph graph, BetweennessCentralityBaseConfig configuration) {
        var params = configuration.toParameters();
        var task = tasks.betweennessCentrality(graph, params);

        var progressTracker =  progressTrackerCreator.createProgressTracker(task,configuration);

        return centralityAlgorithms.betweennessCentrality(graph, params, progressTracker);
    }

    BridgeResult bridges(Graph graph, AlgoBaseConfig configuration, boolean shouldComputeComponents) {

        return centralityAlgorithms.bridges(graph, configuration, shouldComputeComponents);
    }

    public CELFResult celf(Graph graph, InfluenceMaximizationBaseConfig configuration) {

        var params = configuration.toParameters();
        var task = tasks.CELF(graph, params);

        var progressTracker =  progressTrackerCreator.createProgressTracker(task,configuration);

        return centralityAlgorithms.celf(graph, params,progressTracker);
    }

    ClosenessCentralityResult closenessCentrality(Graph graph, ClosenessCentralityBaseConfig configuration) {

        return centralityAlgorithms.closenessCentrality(graph, configuration);
    }

    public ClosenessCentralityResult closenessCentrality(
        Graph graph,
        ClosenessCentralityBaseConfig configuration,
        ProgressTracker progressTracker
    ) {

        return centralityAlgorithms.closenessCentrality(graph, configuration, progressTracker);
    }

    DegreeCentralityResult degreeCentrality(Graph graph, DegreeCentralityConfig configuration) {

        return centralityAlgorithms.degreeCentrality(graph, configuration);
    }

    PageRankResult eigenVector(Graph graph, EigenvectorConfig configuration) {
        return centralityAlgorithms.eigenVector(graph, configuration);
    }

    public PageRankResult eigenVector(
        Graph graph,
        EigenvectorConfig configuration,
        ProgressTracker progressTracker
    ) {
        return centralityAlgorithms.eigenVector(graph, configuration, progressTracker);
    }

    HarmonicResult harmonicCentrality(Graph graph, AlgoBaseConfig configuration) {
        return centralityAlgorithms.harmonicCentrality(graph, configuration);
    }

    public HarmonicResult harmonicCentrality(
        Graph graph,
        ConcurrencyConfig configuration,
        ProgressTracker progressTracker
    ) {
        return centralityAlgorithms.harmonicCentrality(graph, configuration, progressTracker);
    }

    PregelResult hits(Graph graph, HitsConfig configuration) {

        return centralityAlgorithms.hits(graph, configuration);
    }

    IndirectExposureResult indirectExposure(Graph graph, IndirectExposureConfig configuration) {
        return centralityAlgorithms.indirectExposure(graph, configuration);
    }

    public PageRankResult pageRank(Graph graph, PageRankConfig configuration) {
        return centralityAlgorithms.pageRank(graph, configuration);
    }

    public PageRankResult pageRank(Graph graph, PageRankConfig configuration, ProgressTracker progressTracker) {

        return centralityAlgorithms.pageRank(graph, configuration, progressTracker);
    }


}
