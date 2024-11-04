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
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.articulationpoints.ArticulationPointsMutateConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.indirectExposure.IndirectExposureMutateConfig;
import org.neo4j.gds.indirectExposure.IndirectExposureResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticulationPoints;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.CELF;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.IndirectExposure;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SLLPA;

public class CentralityAlgorithmsMutateModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    public CentralityAlgorithmsMutateModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimation,
        CentralityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        ArticleRankMutateConfig configuration,
        ResultBuilder<ArticleRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep<>(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            ArticleRank,
            estimation::pageRank,
            (graph, __) -> algorithms.articleRank(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT articulationPoints(
        GraphName graphName,
        ArticulationPointsMutateConfig configuration,
        ResultBuilder<ArticulationPointsMutateConfig, BitSet, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ArticulationPointsMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            ArticulationPoints,
            estimation::articulationPoints,
            (graph, __) -> algorithms.articulationPoints(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }


    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityMutateConfig configuration,
        ResultBuilder<BetweennessCentralityMutateConfig, BetwennessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new BetweennessCentralityMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            BetweennessCentrality,
            () -> estimation.betweennessCentrality(configuration),
            (graph, __) -> algorithms.betweennessCentrality(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationMutateConfig configuration,
        ResultBuilder<InfluenceMaximizationMutateConfig, CELFResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new CelfMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            CELF,
            () -> estimation.celf(configuration),
            (graph, __) -> algorithms.celf(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityMutateConfig configuration,
        ResultBuilder<ClosenessCentralityMutateConfig, ClosenessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ClosenessCentralityMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            ClosenessCentrality,
            () -> estimation.closenessCentrality(configuration),
            (graph, __) -> algorithms.closenessCentrality(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityMutateConfig configuration,
        ResultBuilder<DegreeCentralityMutateConfig, DegreeCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new DegreeCentralityMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            DegreeCentrality,
            () -> estimation.degreeCentrality(configuration),
            (graph, __) -> algorithms.degreeCentrality(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        EigenvectorMutateConfig configuration,
        ResultBuilder<EigenvectorMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep<>(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            EigenVector,
            estimation::pageRank,
            (graph, __) -> algorithms.eigenVector(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityMutateConfig configuration,
        ResultBuilder<HarmonicCentralityMutateConfig, HarmonicResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HarmonicCentralityMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            HarmonicCentrality,
            estimation::harmonicCentrality,
            (graph, __) -> algorithms.harmonicCentrality(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankMutateConfig configuration,
        ResultBuilder<PageRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep<>(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            PageRank,
            estimation::pageRank,
            (graph, __) -> algorithms.pageRank(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT indirectExposure(
        GraphName graphName,
        IndirectExposureMutateConfig configuration,
        ResultBuilder<IndirectExposureMutateConfig, IndirectExposureResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new IndirectExposureMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            IndirectExposure,
            estimation::indirectExposure,
            (graph, __) -> algorithms.indirectExposure(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT speakerListenerLPA(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        ResultBuilder<SpeakerListenerLPAConfig, PregelResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new SpeakerListenerLPAMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            SLLPA,
            estimation::indirectExposure,
            (graph, __) -> algorithms.speakerListenerLPA(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
