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
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.WriteResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.WriteSideEffect;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutWriteConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingWriteConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.hdbscan.HDBScanWriteConfig;
import org.neo4j.gds.hdbscan.Labels;
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
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.triangle.TriangleCountWriteConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SLLPA;

public final class CommunityAlgorithmsWriteModeBusinessFacade {
    private final WriteNodePropertyService writeNodePropertyService;
    private final InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms;
    private final Synchroniser synchroniser;

    private CommunityAlgorithmsWriteModeBusinessFacade(
        WriteNodePropertyService writeNodePropertyService,
        InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.instrumentedCommunityAlgorithms = instrumentedCommunityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public static CommunityAlgorithmsWriteModeBusinessFacade create(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        InstrumentedCommunityAlgorithms raw,
        Synchroniser synchroniser
    ) {
        var writeToDatabase = new WriteNodePropertyService(log, requestScopedDependencies, writeContext);

        return new CommunityAlgorithmsWriteModeBusinessFacade(
            writeToDatabase,
            raw,
            synchroniser
        );
    }

    public <RESULT> RESULT approxMaxKCut(
        GraphName graphName,
        ApproxMaxKCutWriteConfig configuration,
        ResultBuilder<ApproxMaxKCutWriteConfig, ApproxMaxKCutResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ApproxMaxKCutWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.approximateMaximumKCut(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT cliqueCounting(
        GraphName graphName,
        CliqueCountingWriteConfig configuration,
        ResultBuilder<CliqueCountingWriteConfig, CliqueCountingResult, RESULT, Void> resultBuilder
    ) {
        var writeStep = new CliqueCountingWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.cliqueCounting(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT hdbscan(
        GraphName graphName,
        HDBScanWriteConfig configuration,
        ResultBuilder<HDBScanWriteConfig, Labels, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new HDBScanWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.hdbscan(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringWriteConfig configuration,
        ResultBuilder<K1ColoringWriteConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        var writeStep = new K1ColoringWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.k1Coloring(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionWriteConfig configuration,
        ResultBuilder<KCoreDecompositionWriteConfig, KCoreDecompositionResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KCoreDecompositionWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kCoreDecomposition(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansWriteConfig configuration,
        ResultBuilder<KmeansWriteConfig, KmeansResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new KMeansWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kMeans(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationWriteConfig configuration,
        ResultBuilder<LabelPropagationWriteConfig, LabelPropagationResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LabelPropagationWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.labelPropagation(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientWriteConfig configuration,
        ResultBuilder<LocalClusteringCoefficientWriteConfig, LocalClusteringCoefficientResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LccWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.localClusteringCoefficient(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenWriteConfig configuration,
        ResultBuilder<LeidenWriteConfig, LeidenResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new LeidenWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.leiden(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainWriteConfig configuration,
        ResultBuilder<LouvainWriteConfig, LouvainResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new LouvainWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.louvain(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationWriteConfig configuration,
        ResultBuilder<ModularityOptimizationWriteConfig, ModularityOptimizationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new ModularityOptimizationWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.modularityOptimization(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccWriteConfig configuration,
        ResultBuilder<SccWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.scc(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT sccAlpha(
        GraphName graphName,
        SccAlphaWriteConfig configuration,
        ResultBuilder<SccAlphaWriteConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SccAlphaWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.scc(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT speakerListenerLPA(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        ResultBuilder<SpeakerListenerLPAConfig, PregelResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new SpeakerListenerLPAWriteStep(writeNodePropertyService, configuration, SLLPA);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.speakerListenerLPA(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountWriteConfig configuration,
        ResultBuilder<TriangleCountWriteConfig, TriangleCountResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var writeStep = new TriangleCountWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangleCount(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccWriteConfig configuration,
        ResultBuilder<WccWriteConfig, DisjointSetStruct, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {
        var writeStep = new WccWriteStep(writeNodePropertyService, configuration);

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.wcc(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }
}
