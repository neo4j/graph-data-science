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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.triangle.TriangleStreamResult;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ApproximateMaximumKCut;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Conductance;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.K1Coloring;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KCore;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KMeans;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.LCC;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Leiden;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Louvain;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Modularity;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SCC;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.TriangleCount;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Triangles;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.WCC;

public class CommunityAlgorithmsStreamModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public CommunityAlgorithmsStreamModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    public <RESULT> RESULT approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutStreamConfig configuration,
        ResultBuilder<ApproxMaxKCutStreamConfig, ApproxMaxKCutResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            ApproximateMaximumKCut,
            () -> estimationFacade.approximateMaximumKCut(configuration),
            graph -> algorithms.approximateMaximumKCut(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT conductance(
        GraphName graphName,
        ConductanceStreamConfig configuration,
        ResultBuilder<ConductanceStreamConfig, ConductanceResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Conductance,
            estimationFacade::conductance,
            graph -> algorithms.conductance(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringStreamConfig configuration,
        ResultBuilder<K1ColoringStreamConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            K1Coloring,
            estimationFacade::k1Coloring,
            graph -> algorithms.k1Coloring(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionStreamConfig configuration,
        ResultBuilder<KCoreDecompositionStreamConfig, KCoreDecompositionResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            KCore,
            estimationFacade::kCore,
            graph -> algorithms.kCore(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansStreamConfig configuration,
        ResultBuilder<KmeansStreamConfig, KmeansResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            KMeans,
            () -> estimationFacade.kMeans(configuration),
            graph -> algorithms.kMeans(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationStreamConfig configuration,
        ResultBuilder<LabelPropagationStreamConfig, LabelPropagationResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            LabelPropagation,
            estimationFacade::labelPropagation,
            graph -> algorithms.labelPropagation(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientStreamConfig configuration,
        ResultBuilder<LocalClusteringCoefficientStreamConfig, LocalClusteringCoefficientResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            LCC,
            () -> estimationFacade.lcc(configuration),
            graph -> algorithms.lcc(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenStreamConfig configuration,
        ResultBuilder<LeidenStreamConfig, LeidenResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Leiden,
            () -> estimationFacade.leiden(configuration),
            graph -> algorithms.leiden(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainStreamConfig configuration,
        ResultBuilder<LouvainStreamConfig, LouvainResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Louvain,
            () -> estimationFacade.louvain(configuration),
            graph -> algorithms.louvain(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT modularity(
        GraphName graphName,
        ModularityStreamConfig configuration,
        ResultBuilder<ModularityStreamConfig, ModularityResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Modularity,
            estimationFacade::modularity,
            graph -> algorithms.modularity(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationStreamConfig configuration,
        ResultBuilder<ModularityOptimizationStreamConfig, ModularityOptimizationResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            graph -> algorithms.modularityOptimization(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccStreamConfig configuration,
        ResultBuilder<SccStreamConfig, HugeLongArray, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            SCC,
            estimationFacade::scc,
            graph -> algorithms.scc(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountStreamConfig configuration,
        ResultBuilder<TriangleCountStreamConfig, TriangleCountResult, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            TriangleCount,
            estimationFacade::triangleCount,
            graph -> algorithms.triangleCount(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT triangles(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        ResultBuilder<TriangleCountBaseConfig, Stream<TriangleStreamResult>, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Triangles,
            estimationFacade::triangles,
            graph -> algorithms.triangles(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccStreamConfig configuration,
        ResultBuilder<WccStreamConfig, DisjointSetStruct, RESULT, Void> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            WCC,
            () -> estimationFacade.wcc(configuration),
            graph -> algorithms.wcc(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }
}
