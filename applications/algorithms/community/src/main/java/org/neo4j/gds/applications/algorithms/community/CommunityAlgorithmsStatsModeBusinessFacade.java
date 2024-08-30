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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStatsConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kcore.KCoreDecompositionStatsConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansStatsConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenStatsConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainStatsConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularity.ModularityStatsConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStatsConfig;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStatsConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleCountStatsConfig;
import org.neo4j.gds.wcc.WccStatsConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.K1Coloring;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KCore;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KMeans;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.LCC;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Leiden;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Louvain;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Modularity;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SCC;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.TriangleCount;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.WCC;

public class CommunityAlgorithmsStatsModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithms communityAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    CommunityAlgorithmsStatsModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithms communityAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.communityAlgorithms = communityAlgorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringStatsConfig configuration,
        StatsResultBuilder<K1ColoringStatsConfig, K1ColoringResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            K1Coloring,
            estimationFacade::k1Coloring,
            (graph, __) -> communityAlgorithms.k1Coloring(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionStatsConfig configuration,
        StatsResultBuilder<KCoreDecompositionStatsConfig, KCoreDecompositionResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            KCore,
            estimationFacade::kCore,
            (graph, __) -> communityAlgorithms.kCore(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansStatsConfig configuration,
        StatsResultBuilder<KmeansStatsConfig, KmeansResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            KMeans,
            () -> estimationFacade.kMeans(configuration),
            (graph, __) -> communityAlgorithms.kMeans(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationStatsConfig configuration,
        StatsResultBuilder<LabelPropagationStatsConfig, LabelPropagationResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            LabelPropagation,
            estimationFacade::labelPropagation,
            (graph, __) -> communityAlgorithms.labelPropagation(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientStatsConfig configuration,
        StatsResultBuilder<LocalClusteringCoefficientStatsConfig, LocalClusteringCoefficientResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            LCC,
            () -> estimationFacade.lcc(configuration),
            (graph, __) -> communityAlgorithms.lcc(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenStatsConfig configuration,
        StatsResultBuilder<LeidenStatsConfig, LeidenResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            Leiden,
            () -> estimationFacade.leiden(configuration),
            (graph, __) -> communityAlgorithms.leiden(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainStatsConfig configuration,
        StatsResultBuilder<LouvainStatsConfig, LouvainResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            Louvain,
            () -> estimationFacade.louvain(configuration),
            (graph, __) -> communityAlgorithms.louvain(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT modularity(
        GraphName graphName,
        ModularityStatsConfig configuration,
        StatsResultBuilder<ModularityStatsConfig, ModularityResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            Modularity,
            estimationFacade::modularity,
            (graph, __) -> communityAlgorithms.modularity(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationStatsConfig configuration,
        StatsResultBuilder<ModularityOptimizationStatsConfig, ModularityOptimizationResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            (graph, __) -> communityAlgorithms.modularityOptimization(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccStatsConfig configuration,
        StatsResultBuilder<SccStatsConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            SCC,
            estimationFacade::scc,
            (graph, __) -> communityAlgorithms.scc(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountStatsConfig configuration,
        StatsResultBuilder<TriangleCountStatsConfig, TriangleCountResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            TriangleCount,
            estimationFacade::triangleCount,
            (graph, __) -> communityAlgorithms.triangleCount(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccStatsConfig configuration,
        StatsResultBuilder<WccStatsConfig, DisjointSetStruct, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            WCC,
            () -> estimationFacade.wcc(configuration),
            (graph, __) -> communityAlgorithms.wcc(graph, configuration),
            resultBuilder
        );
    }
}
