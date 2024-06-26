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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
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

import java.util.Optional;

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

public final class CommunityAlgorithmsWriteModeBusinessFacade {
    private final CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final CommunityAlgorithms algorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final WriteToDatabase writeToDatabase;

    private CommunityAlgorithmsWriteModeBusinessFacade(
        CommunityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        WriteToDatabase writeToDatabase
    ) {
        this.estimationFacade = estimationFacade;
        this.algorithms = algorithms;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.writeToDatabase = writeToDatabase;
    }

    public static CommunityAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        CommunityAlgorithmsEstimationModeBusinessFacade estimation,
        CommunityAlgorithms algorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        var writeNodePropertyService = new WriteNodePropertyService(log, requestScopedDependencies, writeContext);
        var writeToDatabase = new WriteToDatabase(writeNodePropertyService);

        return new CommunityAlgorithmsWriteModeBusinessFacade(
            estimation,
            algorithms,
            algorithmProcessingTemplate,
            writeToDatabase
        );
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringWriteConfig configuration,
        ResultBuilder<K1ColoringWriteConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        var writeStep = new K1ColoringWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            K1Coloring,
            estimationFacade::k1Coloring,
            graph -> algorithms.k1Coloring(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionWriteConfig configuration,
        ResultBuilder<KCoreDecompositionWriteConfig, KCoreDecompositionResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KCoreWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            KCore,
            estimationFacade::kCore,
            graph -> algorithms.kCore(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansWriteConfig configuration,
        ResultBuilder<KmeansWriteConfig, KmeansResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KMeansWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            KMeans,
            () -> estimationFacade.kMeans(configuration),
            graph -> algorithms.kMeans(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationWriteConfig configuration,
        ResultBuilder<LabelPropagationWriteConfig, LabelPropagationResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LabelPropagationWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            LabelPropagation,
            estimationFacade::labelPropagation,
            graph -> algorithms.labelPropagation(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientWriteConfig configuration,
        ResultBuilder<LocalClusteringCoefficientWriteConfig, LocalClusteringCoefficientResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            LCC,
            () -> estimationFacade.lcc(configuration),
            graph -> algorithms.lcc(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenWriteConfig configuration,
        ResultBuilder<LeidenWriteConfig, LeidenResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LeidenWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Leiden,
            () -> estimationFacade.leiden(configuration),
            graph -> algorithms.leiden(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainWriteConfig configuration,
        ResultBuilder<LouvainWriteConfig, LouvainResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LouvainWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            Louvain,
            () -> estimationFacade.louvain(configuration),
            graph -> algorithms.louvain(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationWriteConfig configuration,
        ResultBuilder<ModularityOptimizationWriteConfig, ModularityOptimizationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ModularityOptimizationWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            ModularityOptimization,
            estimationFacade::modularityOptimization,
            graph -> algorithms.modularityOptimization(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccWriteConfig configuration,
        ResultBuilder<SccWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            SCC,
            estimationFacade::scc,
            graph -> algorithms.scc(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT sccAlpha(
        GraphName graphName,
        SccAlphaWriteConfig configuration,
        ResultBuilder<SccAlphaWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccAlphaWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            SCC,
            estimationFacade::scc,
            graph -> algorithms.scc(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountWriteConfig configuration,
        ResultBuilder<TriangleCountWriteConfig, TriangleCountResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new TriangleCountWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            TriangleCount,
            estimationFacade::triangleCount,
            graph -> algorithms.triangleCount(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccWriteConfig configuration,
        ResultBuilder<WccWriteConfig, DisjointSetStruct, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new WccWriteStep(writeToDatabase, configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            Optional.empty(),
            WCC,
            () -> estimationFacade.wcc(configuration),
            graph -> algorithms.wcc(graph, configuration),
            Optional.of(writeStep),
            resultBuilder
        );
    }
}
