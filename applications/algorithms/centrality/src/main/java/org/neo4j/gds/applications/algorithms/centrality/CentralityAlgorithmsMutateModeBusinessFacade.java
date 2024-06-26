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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.betweenness.BetweennessCentralityMutateConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityMutateConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityMutateConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.harmonic.HarmonicCentralityMutateConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.pagerank.PageRankResult;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ArticleRank;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BetweennessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.CELF;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ClosenessCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DegreeCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.EigenVector;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.HarmonicCentrality;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.PageRank;

public class CentralityAlgorithmsMutateModeBusinessFacade {
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CentralityAlgorithms algorithms;
    private final AlgorithmProcessingTemplate template;
    private final MutateNodeProperty mutateNodeProperty;

    public CentralityAlgorithmsMutateModeBusinessFacade(
        CentralityAlgorithmsEstimationModeBusinessFacade estimation,
        CentralityAlgorithms algorithms,
        AlgorithmProcessingTemplate template,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.template = template;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT articleRank(
        GraphName graphName,
        PageRankMutateConfig configuration,
        ResultBuilder<PageRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            ArticleRank,
            estimation::pageRank,
            graph -> algorithms.articleRank(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT betweennessCentrality(
        GraphName graphName,
        BetweennessCentralityMutateConfig configuration,
        ResultBuilder<BetweennessCentralityMutateConfig, BetwennessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new BetweennessCentralityMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            BetweennessCentrality,
            () -> estimation.betweennessCentrality(configuration),
            graph -> algorithms.betweennessCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT celf(
        GraphName graphName,
        InfluenceMaximizationMutateConfig configuration,
        ResultBuilder<InfluenceMaximizationMutateConfig, CELFResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new CelfMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            CELF,
            () -> estimation.celf(configuration),
            graph -> algorithms.celf(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT closenessCentrality(
        GraphName graphName,
        ClosenessCentralityMutateConfig configuration,
        ResultBuilder<ClosenessCentralityMutateConfig, ClosenessCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ClosenessCentralityMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            ClosenessCentrality,
            () -> estimation.closenessCentrality(configuration),
            graph -> algorithms.closenessCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT degreeCentrality(
        GraphName graphName,
        DegreeCentralityMutateConfig configuration,
        ResultBuilder<DegreeCentralityMutateConfig, DegreeCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new DegreeCentralityMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            DegreeCentrality,
            () -> estimation.degreeCentrality(configuration),
            graph -> algorithms.degreeCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT eigenVector(
        GraphName graphName,
        PageRankMutateConfig configuration,
        ResultBuilder<PageRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            EigenVector,
            estimation::pageRank,
            graph -> algorithms.eigenVector(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT harmonicCentrality(
        GraphName graphName,
        HarmonicCentralityMutateConfig configuration,
        ResultBuilder<HarmonicCentralityMutateConfig, HarmonicResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HarmonicCentralityMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            HarmonicCentrality,
            estimation::harmonicCentrality,
            graph -> algorithms.harmonicCentrality(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT pageRank(
        GraphName graphName,
        PageRankMutateConfig configuration,
        ResultBuilder<PageRankMutateConfig, PageRankResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new PageRankMutateStep(mutateNodeProperty, configuration);

        return template.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            PageRank,
            estimation::pageRank,
            graph -> algorithms.pageRank(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }
}
