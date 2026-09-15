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

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.WriteSideEffect;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.ArticulationPointsWriteConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityWriteConfig;
import org.neo4j.gds.centrality.ArticulationPointsWriteStep;
import org.neo4j.gds.centrality.BetweennessCentralityWriteStep;
import org.neo4j.gds.centrality.CelfWriteStep;
import org.neo4j.gds.centrality.ClosenessCentralityWriteStep;
import org.neo4j.gds.centrality.DegreeCentralityWriteStep;
import org.neo4j.gds.centrality.GenericRankWriteStep;
import org.neo4j.gds.centrality.HarmonicCentralityWriteStep;
import org.neo4j.gds.centrality.HitsWriteStep;
import org.neo4j.gds.closeness.ClosenessCentralityWriteConfig;
import org.neo4j.gds.degree.DegreeCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityWriteConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pagerank.ArticleRankWriteConfig;
import org.neo4j.gds.pagerank.EigenvectorWriteConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.PageRankWriteConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.CELF;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public final class CentralityAlgorithmsWriteModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final TrackedCentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final WriteNodePropertyService writeNodePropertyService;
    private final InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms;
    private final Synchroniser synchroniser;

    private CentralityAlgorithmsWriteModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        TrackedCentralityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        WriteNodePropertyService writeNodePropertyService,
        InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.writeNodePropertyService = writeNodePropertyService;
        this.instrumentedCentralityAlgorithms = instrumentedCentralityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public static CentralityAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        TrackedCentralityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms,
        Synchroniser synchroniser
    ) {
        var writeToDatabase = new WriteNodePropertyService(log, requestScopedDependencies, writeContext);

        return new CentralityAlgorithmsWriteModeBusinessFacade(
            estimationFacade,
            algorithms,
            algorithmProcessingTemplateConvenience,
            writeToDatabase,
            instrumentedCentralityAlgorithms,
            synchroniser
        );
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankWriteConfig configuration,
        ResultBuilder<ArticleRankWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new GenericRankWriteStep(
            writeNodePropertyService,
            configuration.writeProperty(),
            configuration.writeConcurrency(),
            configuration::resolveResultStore,
            ArticleRank
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ArticleRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.articleRank(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <CONFIGURATION extends ArticulationPointsWriteConfig, RESULT> RESULT articulationPoints(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, ArticulationPointsResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        // this is the value add for this layer
        var writeStep = new ArticulationPointsWriteStep(
            writeNodePropertyService,
            configuration::resolveResultStore,
            configuration.writeConcurrency(),
            configuration.writeProperty()
        );

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articulationPoints(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder), // and this
            false
        ));
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityWriteConfig configuration,
        ResultBuilder<BetweennessCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new BetweennessCentralityWriteStep(writeNodePropertyService, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimationFacade.betweennessCentrality(configuration),
            (graph, __) -> algorithms.betweennessCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <CONFIGURATION extends InfluenceMaximizationWriteConfig, RESULT> RESULT celf(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, CELFResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new CelfWriteStep(
            writeNodePropertyService,
            configuration::resolveResultStore,
            configuration.writeConcurrency(),
            configuration.writeProperty()
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            CELF,
            () -> estimationFacade.celf(configuration),
            (graph, __) -> algorithms.celf(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityWriteConfig configuration,
        ResultBuilder<ClosenessCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ClosenessCentralityWriteStep(
            writeNodePropertyService,
            configuration.writeProperty(),
            configuration.writeConcurrency(),
            configuration::resolveResultStore
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ClosenessCentrality,
            estimationFacade::closenessCentrality,
            (graph, __) -> algorithms.closenessCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityWriteConfig configuration,
        ResultBuilder<DegreeCentralityWriteConfig, CentralityAlgorithmResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new DegreeCentralityWriteStep(writeNodePropertyService, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimationFacade.degreeCentrality(configuration),
            (graph, __) -> algorithms.degreeCentrality(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenvector(
        GraphName graphName,
        EigenvectorWriteConfig configuration,
        ResultBuilder<EigenvectorWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new GenericRankWriteStep(
            writeNodePropertyService,
            configuration.writeProperty(),
            configuration.writeConcurrency(),
            configuration::resolveResultStore,
            EigenVector
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            EigenVector,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.eigenVector(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <CONFIGURATION extends HarmonicCentralityWriteConfig, RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, HarmonicResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new HarmonicCentralityWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.harmonicCentrality(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT hits(
        GraphName graphName,
        HitsConfig configuration,
        ResultBuilder<HitsConfig, PregelResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new HitsWriteStep(
            writeNodePropertyService,
            configuration::resolveResultStore,
            configuration.writeConcurrency(),
            configuration.authProperty(),
            configuration.hubProperty(),
            configuration.writeProperty()
        );

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.hits(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankWriteConfig configuration,
        ResultBuilder<PageRankWriteConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new GenericRankWriteStep(
            writeNodePropertyService,
            configuration.writeProperty(),
            configuration.writeConcurrency(),
            configuration::resolveResultStore,
            PageRank
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            PageRank,
            estimationFacade::pageRank,
            (graph, __) -> algorithms.pageRank(graph, configuration),
            writeStep,
            resultBuilder
        );
    }
}
