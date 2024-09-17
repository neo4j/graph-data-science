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
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsWriteConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.closeness.ClosenessCentralityWriteConfig;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pagerank.ArticleRankWriteConfig;
import org.neo4j.gds.pagerank.EigenvectorWriteConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankWriteConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ArticleRank;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ArticulationPoints;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.CELF;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.EigenVector;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.PageRank;

public final class CentralityAlgorithmsWriteModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CentralityAlgorithms centralityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final WriteToDatabase writeToDatabase;

    private CentralityAlgorithmsWriteModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        WriteToDatabase writeToDatabase
    ) {
        this.estimationFacade = estimationFacade;
        this.centralityAlgorithms = centralityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.writeToDatabase = writeToDatabase;
    }

    public static CentralityAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CentralityAlgorithms centralityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        var writeToDatabase = new WriteToDatabase(log, requestScopedDependencies, writeContext);

        return new CentralityAlgorithmsWriteModeBusinessFacade(
            estimationFacade,
            centralityAlgorithms,
            algorithmProcessingTemplateConvenience,
            writeToDatabase
        );
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankWriteConfig configuration,
        ResultBuilder<ArticleRankWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new PageRankWriteStep<>(writeToDatabase, configuration, ArticleRank);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.articleRank(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityWriteConfig configuration,
        ResultBuilder<BetweennessCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new BetweennessCentralityWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            (graph, __) -> centralityAlgorithms.betweennessCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT articulationPoints(
        GraphName graphName,
        ArticulationPointsWriteConfig configuration,
        ResultBuilder<ArticulationPointsWriteConfig, BitSet, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ArticulationPoints,
            estimationFacade::articulationPoints,
            (graph, __) -> centralityAlgorithms.articulationPoints(graph, configuration),
            new ArticulationPointsWriteStep(configuration, writeToDatabase),
            resultBuilder
        );
    }

    public <CONFIGURATION extends InfluenceMaximizationWriteConfig, RESULT> RESULT celf(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, CELFResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new CelfWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            (graph, __) -> centralityAlgorithms.celf(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityWriteConfig configuration,
        ResultBuilder<ClosenessCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ClosenessCentralityWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ClosenessCentrality,
            () -> estimationFacade.closenessCentrality(configuration),
            (graph, __) -> centralityAlgorithms.closenessCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityWriteConfig configuration,
        ResultBuilder<DegreeCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new DegreeCentralityWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            (graph, __) -> centralityAlgorithms.degreeCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenvector(
        GraphName graphName,
        EigenvectorWriteConfig configuration,
        ResultBuilder<EigenvectorWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new PageRankWriteStep<>(writeToDatabase, configuration, EigenVector);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.eigenVector(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <CONFIGURATION extends HarmonicCentralityWriteConfig, RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, HarmonicResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new HarmonicCentralityWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            HarmonicCentrality,
            estimationFacade::harmonicCentrality,
            (graph, __) -> centralityAlgorithms.harmonicCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankWriteConfig configuration,
        ResultBuilder<PageRankWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new PageRankWriteStep<>(writeToDatabase, configuration, ArticleRank);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            (graph, __) -> centralityAlgorithms.pageRank(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

}
