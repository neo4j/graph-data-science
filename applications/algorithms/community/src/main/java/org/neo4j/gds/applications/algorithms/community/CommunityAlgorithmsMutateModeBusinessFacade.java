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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMutateConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.wcc.WccMutateConfig;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ApproximateMaximumKCut;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.K1Coloring;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KCore;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KMeans;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.LCC;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Leiden;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Louvain;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SCC;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.TriangleCount;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.WCC;

public class CommunityAlgorithmsMutateModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimation;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    public CommunityAlgorithmsMutateModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.estimation = estimation;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutMutateConfig configuration,
        ResultBuilder<ApproxMaxKCutMutateConfig, ApproxMaxKCutResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ApproxMaxKCutMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            ApproximateMaximumKCut,
            () -> estimation.approximateMaximumKCut(configuration),
            (graph, __) -> algorithms.approximateMaximumKCut(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringMutateConfig configuration,
        ResultBuilder<K1ColoringMutateConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        var mutateStep = new K1ColoringMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            K1Coloring,
            estimation::k1Coloring,
            (graph, __) -> algorithms.k1Coloring(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionMutateConfig configuration,
        ResultBuilder<KCoreDecompositionMutateConfig, KCoreDecompositionResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new KCoreMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            KCore,
            estimation::kCore,
            (graph, __) -> algorithms.kCore(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansMutateConfig configuration,
        ResultBuilder<KmeansMutateConfig, KmeansResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new KMeansMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            KMeans,
            () -> estimation.kMeans(configuration),
            (graph, __) -> algorithms.kMeans(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationMutateConfig configuration,
        ResultBuilder<LabelPropagationMutateConfig, LabelPropagationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new LabelPropagationMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            LabelPropagation,
            estimation::labelPropagation,
            (graph, __) -> algorithms.labelPropagation(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientMutateConfig configuration,
        ResultBuilder<LocalClusteringCoefficientMutateConfig, LocalClusteringCoefficientResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new LccMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            LCC,
            () -> estimation.lcc(configuration),
            (graph, __) -> algorithms.lcc(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenMutateConfig configuration,
        ResultBuilder<LeidenMutateConfig, LeidenResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var mutateStep = new LeidenMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            Leiden,
            () -> estimation.leiden(configuration),
            (graph, __) -> algorithms.leiden(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainMutateConfig configuration,
        ResultBuilder<LouvainMutateConfig, LouvainResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new LouvainMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            Louvain,
            () -> estimation.louvain(configuration),
            (graph, __) -> algorithms.louvain(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationMutateConfig configuration,
        ResultBuilder<ModularityOptimizationMutateConfig, ModularityOptimizationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ModularityOptimizationMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            ModularityOptimization,
            estimation::modularityOptimization,
            (graph, __) -> algorithms.modularityOptimization(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccMutateConfig configuration,
        ResultBuilder<SccMutateConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new SccMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            SCC,
            estimation::scc,
            (graph, __) -> algorithms.scc(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountMutateConfig configuration,
        ResultBuilder<TriangleCountMutateConfig, TriangleCountResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new TriangleCountMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            TriangleCount,
            estimation::triangleCount,
            (graph, __) -> algorithms.triangleCount(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccMutateConfig configuration,
        ResultBuilder<WccMutateConfig, DisjointSetStruct, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new WccMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            WCC,
            () -> estimation.wcc(configuration),
            (graph, __) -> algorithms.wcc(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
