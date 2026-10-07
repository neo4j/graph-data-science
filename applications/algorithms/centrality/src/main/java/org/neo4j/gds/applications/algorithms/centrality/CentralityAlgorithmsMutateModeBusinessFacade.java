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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.MutateSideEffect;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.centrality.ArticulationPointsMutateStep;
import org.neo4j.gds.centrality.BetweennessCentralityMutateStep;
import org.neo4j.gds.centrality.CelfMutateStep;
import org.neo4j.gds.centrality.ClosenessCentralityMutateStep;
import org.neo4j.gds.centrality.DegreeCentralityMutateStep;
import org.neo4j.gds.centrality.GenericRankMutateStep;
import org.neo4j.gds.centrality.HarmonicCentralityMutateStep;
import org.neo4j.gds.centrality.HitsMutateStep;
import org.neo4j.gds.centrality.IndirectExposureMutateStep;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankResult;

import java.util.Optional;

public class CentralityAlgorithmsMutateModeBusinessFacade {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms;
    private final Synchroniser synchroniser;

    public CentralityAlgorithmsMutateModeBusinessFacade(
        MutateNodePropertyService mutateNodePropertyService,
        InstrumentedCentralityAlgorithms instrumentedCentralityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.instrumentedCentralityAlgorithms = instrumentedCentralityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankMutateConfig configuration,
        ResultBuilder<ArticleRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new GenericRankMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articleRank(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <CONFIGURATION extends ArticulationPointsMutateConfig, RESULT> RESULT articulationPoints(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, ArticulationPointsResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        // this is the value add for this layer
        var mutateStep = new ArticulationPointsMutateStep(
            mutateNodePropertyService,
            configuration.mutateProperty(),
            configuration.nodeLabels()
        );

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.articulationPoints(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)), // and this
            new MutateResultRenderer<>(configuration, resultBuilder), // and this
            false
        ));
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityMutateConfig configuration,
        ResultBuilder<BetweennessCentralityMutateConfig, BetwennessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new BetweennessCentralityMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.betweennessCentrality(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationMutateConfig configuration,
        ResultBuilder<InfluenceMaximizationMutateConfig, CELFResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new CelfMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.celf(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityMutateConfig configuration,
        ResultBuilder<ClosenessCentralityMutateConfig, ClosenessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ClosenessCentralityMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.closenessCentrality(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityMutateConfig configuration,
        ResultBuilder<DegreeCentralityMutateConfig, DegreeCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new DegreeCentralityMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.degreeCentrality(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        EigenvectorMutateConfig configuration,
        ResultBuilder<EigenvectorMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new GenericRankMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.eigenVector(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <CONFIGURATION extends HarmonicCentralityMutateConfig, RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        CONFIGURATION configuration,
        ResultBuilder<CONFIGURATION, HarmonicResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HarmonicCentralityMutateStep(
            mutateNodePropertyService,
            configuration.mutateProperty(),
            configuration.nodeLabels()
        );

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.harmonicCentrality(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT hits(
        GraphName graphName,
        HitsConfig configuration,
        ResultBuilder<HitsConfig, PregelResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HitsMutateStep(
            mutateNodePropertyService,
            configuration.authProperty(),
            configuration.hubProperty(),
            configuration.mutateProperty(),
            configuration.nodeLabels()
        );

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.hits(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT indirectExposure(
        GraphName graphName,
        IndirectExposureMutateConfig configuration,
        ResultBuilder<IndirectExposureMutateConfig, IndirectExposureResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new IndirectExposureMutateStep(mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperties().exposures(),
            configuration.mutateProperties().hops(),
            configuration.mutateProperties().parents(),
            configuration.mutateProperties().roots());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.indirectExposure(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankMutateConfig configuration,
        ResultBuilder<PageRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new GenericRankMutateStep(mutateNodePropertyService, configuration.mutateProperty(), configuration.nodeLabels());

        return synchroniser.synchronise(() -> instrumentedCentralityAlgorithms.pageRank(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }
}
