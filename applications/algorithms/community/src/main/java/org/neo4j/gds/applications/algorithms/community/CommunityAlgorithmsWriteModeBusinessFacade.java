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
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteToDatabase;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringWriteConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kcore.KCoreDecompositionWriteConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansWriteConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationWriteConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainWriteConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationWriteConfig;
import org.neo4j.gds.scc.SccAlphaWriteConfig;
import org.neo4j.gds.scc.SccWriteConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleCountWriteConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.K1Coloring;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KCore;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.KMeans;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.LCC;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.LabelPropagation;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Leiden;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Louvain;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.ModularityOptimization;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SCC;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.TriangleCount;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.WCC;

public final class CommunityAlgorithmsWriteModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final WriteToDatabase writeToDatabase;

    private CommunityAlgorithmsWriteModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        WriteToDatabase writeToDatabase
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.writeToDatabase = writeToDatabase;
    }

    public static CommunityAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        var writeToDatabase = new WriteToDatabase(log, requestScopedDependencies, writeContext);

        return new CommunityAlgorithmsWriteModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplateConvenience,
            writeToDatabase
        );
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringWriteConfig configuration,
        ResultBuilder<K1ColoringWriteConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        var writeStep = new K1ColoringWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            K1Coloring,
            estimationFacade::k1Coloring,
            (graph, __) -> algorithms.k1Coloring(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionWriteConfig configuration,
        ResultBuilder<KCoreDecompositionWriteConfig, KCoreDecompositionResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KCoreWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            KCore,
            estimationFacade::kCore,
            (graph, __) -> algorithms.kCore(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansWriteConfig configuration,
        ResultBuilder<KmeansWriteConfig, KmeansResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KMeansWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            KMeans,
            () -> estimationFacade.kMeans(configuration),
            (graph, __) -> algorithms.kMeans(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationWriteConfig configuration,
        ResultBuilder<LabelPropagationWriteConfig, LabelPropagationResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LabelPropagationWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            LabelPropagation,
            estimationFacade::labelPropagation,
            (graph, __) -> algorithms.labelPropagation(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientWriteConfig configuration,
        ResultBuilder<LocalClusteringCoefficientWriteConfig, LocalClusteringCoefficientResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            LCC,
            () -> estimationFacade.lcc(configuration),
            (graph, __) -> algorithms.lcc(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenWriteConfig configuration,
        ResultBuilder<LeidenWriteConfig, LeidenResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LeidenWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            Leiden,
            () -> estimationFacade.leiden(configuration),
            (graph, __) -> algorithms.leiden(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainWriteConfig configuration,
        ResultBuilder<LouvainWriteConfig, LouvainResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LouvainWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            Louvain,
            () -> estimationFacade.louvain(configuration),
            (graph, __) -> algorithms.louvain(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationWriteConfig configuration,
        ResultBuilder<ModularityOptimizationWriteConfig, ModularityOptimizationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ModularityOptimizationWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            (graph, __) -> algorithms.modularityOptimization(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccWriteConfig configuration,
        ResultBuilder<SccWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            SCC,
            estimationFacade::scc,
            (graph, __) -> algorithms.scc(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT sccAlpha(
        GraphName graphName,
        SccAlphaWriteConfig configuration,
        ResultBuilder<SccAlphaWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccAlphaWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            SCC,
            estimationFacade::scc,
            (graph, __) -> algorithms.scc(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountWriteConfig configuration,
        ResultBuilder<TriangleCountWriteConfig, TriangleCountResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new TriangleCountWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            TriangleCount,
            estimationFacade::triangleCount,
            (graph, __) -> algorithms.triangleCount(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccWriteConfig configuration,
        ResultBuilder<WccWriteConfig, DisjointSetStruct, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new WccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            WCC,
            () -> estimationFacade.wcc(configuration),
            (graph, __) -> algorithms.wcc(graph, configuration),
            writeStep,
            resultBuilder
        );
    }
}
