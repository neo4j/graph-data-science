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
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
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
import org.neo4j.gds.triangle.TriangleResult;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ApproximateMaximumKCut;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Conductance;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.K1Coloring;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.KCore;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.KMeans;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.LCC;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Leiden;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Louvain;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Modularity;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SCC;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.TriangleCount;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Triangles;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.WCC;

public class CommunityAlgorithmsStreamModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    CommunityAlgorithmsStreamModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> Stream<RESULT> approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutStreamConfig configuration,
        StreamResultBuilder<ApproxMaxKCutResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ApproximateMaximumKCut,
            () -> estimationFacade.approximateMaximumKCut(configuration),
            (graph, __) -> algorithms.approximateMaximumKCut(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> conductance(
        GraphName graphName,
        ConductanceStreamConfig configuration,
        StreamResultBuilder<ConductanceResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Conductance,
            estimationFacade::conductance,
            (graph, __) -> algorithms.conductance(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> k1Coloring(
        GraphName graphName,
        K1ColoringStreamConfig configuration,
        StreamResultBuilder<K1ColoringResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            K1Coloring,
            estimationFacade::k1Coloring,
            (graph, __) -> algorithms.k1Coloring(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> kCore(
        GraphName graphName,
        KCoreDecompositionStreamConfig configuration,
        StreamResultBuilder<KCoreDecompositionResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            KCore,
            estimationFacade::kCore,
            (graph, __) -> algorithms.kCore(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> kMeans(
        GraphName graphName,
        KmeansStreamConfig configuration,
        StreamResultBuilder<KmeansResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            KMeans,
            () -> estimationFacade.kMeans(configuration),
            (graph, __) -> algorithms.kMeans(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> labelPropagation(
        GraphName graphName,
        LabelPropagationStreamConfig configuration,
        StreamResultBuilder<LabelPropagationResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LabelPropagation,
            estimationFacade::labelPropagation,
            (graph, __) -> algorithms.labelPropagation(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> lcc(
        GraphName graphName,
        LocalClusteringCoefficientStreamConfig configuration,
        StreamResultBuilder<LocalClusteringCoefficientResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            LCC,
            () -> estimationFacade.lcc(configuration),
            (graph, __) -> algorithms.lcc(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> leiden(
        GraphName graphName,
        LeidenStreamConfig configuration,
        StreamResultBuilder<LeidenResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Leiden,
            () -> estimationFacade.leiden(configuration),
            (graph, __) -> algorithms.leiden(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> louvain(
        GraphName graphName,
        LouvainStreamConfig configuration,
        StreamResultBuilder<LouvainResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Louvain,
            () -> estimationFacade.louvain(configuration),
            (graph, __) -> algorithms.louvain(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> modularity(
        GraphName graphName,
        ModularityStreamConfig configuration,
        StreamResultBuilder<ModularityResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Modularity,
            estimationFacade::modularity,
            (graph, __) -> algorithms.modularity(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> modularityOptimization(
        GraphName graphName,
        ModularityOptimizationStreamConfig configuration,
        StreamResultBuilder<ModularityOptimizationResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            (graph, __) -> algorithms.modularityOptimization(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> scc(
        GraphName graphName,
        SccStreamConfig configuration,
        StreamResultBuilder<HugeLongArray, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            SCC,
            estimationFacade::scc,
            (graph, __) -> algorithms.scc(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> triangleCount(
        GraphName graphName,
        TriangleCountStreamConfig configuration,
        StreamResultBuilder<TriangleCountResult, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            TriangleCount,
            estimationFacade::triangleCount,
            (graph, __) -> algorithms.triangleCount(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> triangles(
        GraphName graphName,
        TriangleCountBaseConfig configuration,
        StreamResultBuilder<Stream<TriangleResult>, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            Triangles,
            estimationFacade::triangles,
            (graph, __) -> algorithms.triangles(graph, configuration),
            streamResultBuilder
        );
    }

    public <RESULT> Stream<RESULT> wcc(
        GraphName graphName,
        WccStreamConfig configuration,
        StreamResultBuilder<DisjointSetStruct, RESULT> streamResultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStreamMode(
            graphName,
            configuration,
            WCC,
            () -> estimationFacade.wcc(configuration),
            (graph, __) -> algorithms.wcc(graph, configuration),
            streamResultBuilder
        );
    }
}
