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
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.MutateSideEffect;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.cliquecounting.CliqueCountingMutateConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.community.ApproxMaxKCutMutateStep;
import org.neo4j.gds.community.CliqueCountingMutateStep;
import org.neo4j.gds.community.HDBScanMutateStep;
import org.neo4j.gds.community.K1ColoringMutateStep;
import org.neo4j.gds.community.KCoreMutateStep;
import org.neo4j.gds.community.KMeansMutateStep;
import org.neo4j.gds.community.LabelPropagationMutateStep;
import org.neo4j.gds.community.LccMutateStep;
import org.neo4j.gds.community.LeidenMutateStep;
import org.neo4j.gds.community.LouvainMutateStep;
import org.neo4j.gds.community.ModularityOptimizationMutateStep;
import org.neo4j.gds.community.SccMutateStep;
import org.neo4j.gds.community.SpeakerListenerLPAMutateStep;
import org.neo4j.gds.community.StandardCommunityProperties;
import org.neo4j.gds.community.TriangleCentralityMutateStep;
import org.neo4j.gds.community.TriangleCountMutateStep;
import org.neo4j.gds.community.WccMutateStep;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.hdbscan.HDBScanMutateConfig;
import org.neo4j.gds.hdbscan.Labels;
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
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCentralityMutateConfig;
import org.neo4j.gds.triangle.TriangleCentralityResult;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Optional;

public class CommunityAlgorithmsMutateModeBusinessFacade {
    private final MutateNodePropertyService mutateNodePropertyService;
    private final InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms;
    private final Synchroniser synchroniser;

    public CommunityAlgorithmsMutateModeBusinessFacade(
        MutateNodePropertyService mutateNodePropertyService,
        InstrumentedCommunityAlgorithms instrumentedCommunityAlgorithms,
        Synchroniser synchroniser
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.instrumentedCommunityAlgorithms = instrumentedCommunityAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT approximateMaximumKCut(
        GraphName graphName,
        ApproxMaxKCutMutateConfig configuration,
        ResultBuilder<ApproxMaxKCutMutateConfig, ApproxMaxKCutResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new ApproxMaxKCutMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.approximateMaximumKCut(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT cliqueCounting(
        GraphName graphName,
        CliqueCountingMutateConfig configuration,
        ResultBuilder<CliqueCountingMutateConfig, CliqueCountingResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new CliqueCountingMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.cliqueCounting(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT hdbscan(
        GraphName graphName,
        HDBScanMutateConfig configuration,
        ResultBuilder<HDBScanMutateConfig, Labels, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new HDBScanMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.hdbscan(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT k1Coloring(
        GraphName graphName,
        K1ColoringMutateConfig configuration,
        ResultBuilder<K1ColoringMutateConfig, K1ColoringResult, RESULT, Void> resultBuilder
    ) {
        var mutateStep = new K1ColoringMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.k1Coloring(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT kCore(
        GraphName graphName,
        KCoreDecompositionMutateConfig configuration,
        ResultBuilder<KCoreDecompositionMutateConfig, KCoreDecompositionResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new KCoreMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kCoreDecomposition(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT kMeans(
        GraphName graphName,
        KmeansMutateConfig configuration,
        ResultBuilder<KmeansMutateConfig, KmeansResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new KMeansMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.kMeans(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT labelPropagation(
        GraphName graphName,
        LabelPropagationMutateConfig configuration,
        ResultBuilder<LabelPropagationMutateConfig, LabelPropagationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var standardCommunitiesProperties =  createNodePropertyCalculator(configuration);

        var mutateStep = new LabelPropagationMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            standardCommunitiesProperties
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.labelPropagation(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT lcc(
        GraphName graphName,
        LocalClusteringCoefficientMutateConfig configuration,
        ResultBuilder<LocalClusteringCoefficientMutateConfig, LocalClusteringCoefficientResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new LccMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.localClusteringCoefficient(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT leiden(
        GraphName graphName,
        LeidenMutateConfig configuration,
        ResultBuilder<LeidenMutateConfig, LeidenResult, RESULT, Pair<NodePropertiesWritten, NodePropertyValues>> resultBuilder
    ) {

        var standardCommunitiesProperties =  createNodePropertyCalculator(configuration);

        var mutateStep = new LeidenMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            standardCommunitiesProperties,
            configuration.includeIntermediateCommunities()
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.leiden(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT louvain(
        GraphName graphName,
        LouvainMutateConfig configuration,
        ResultBuilder<LouvainMutateConfig, LouvainResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {

        var standardCommunitiesProperties =  createNodePropertyCalculator(configuration);

        var mutateStep = new LouvainMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            standardCommunitiesProperties,
            configuration.includeIntermediateCommunities()
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.louvain(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT modularityOptimization(
        GraphName graphName,
        ModularityOptimizationMutateConfig configuration,
        ResultBuilder<ModularityOptimizationMutateConfig, ModularityOptimizationResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var standardCommunitiesProperties =  createNodePropertyCalculator(configuration);

        var mutateStep = new ModularityOptimizationMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            standardCommunitiesProperties
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.modularityOptimization(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT scc(
        GraphName graphName,
        SccMutateConfig configuration,
        ResultBuilder<SccMutateConfig, HugeLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new SccMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            configuration.consecutiveIds()
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.scc(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT speakerListenerLPA(
        GraphName graphName,
        SpeakerListenerLPAConfig configuration,
        ResultBuilder<SpeakerListenerLPAConfig, PregelResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new SpeakerListenerLPAMutateStep(mutateNodePropertyService, configuration.nodeLabels(),configuration.mutateProperty());

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.speakerListenerLPA(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT triangleCentrality(
        GraphName graphName,
        TriangleCentralityMutateConfig configuration,
        ResultBuilder<TriangleCentralityMutateConfig, TriangleCentralityResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new TriangleCentralityMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty()
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangleCentrality(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT triangleCount(
        GraphName graphName,
        TriangleCountMutateConfig configuration,
        ResultBuilder<TriangleCountMutateConfig, TriangleCountResult, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new TriangleCountMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty()
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.triangleCount(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT wcc(
        GraphName graphName,
        WccMutateConfig configuration,
        ResultBuilder<WccMutateConfig, DisjointSetStruct, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var standardCommunitiesProperties =  createNodePropertyCalculator(configuration);

        var mutateStep = new WccMutateStep(
            mutateNodePropertyService,
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            standardCommunitiesProperties
        );

        return synchroniser.synchronise(() -> instrumentedCommunityAlgorithms.wcc(
            graphName,
            configuration,
            Optional.of(new MutateSideEffect<>(mutateStep)),
            new MutateResultRenderer<>(configuration, resultBuilder)
        ));
    }

    private static <C extends MutateNodePropertyConfig & SeedConfig & ConsecutiveIdsConfig> StandardCommunityProperties createNodePropertyCalculator(C config){
        return new StandardCommunityProperties(
            config.isIncremental(),
            config.seedProperty(),
            config.consecutiveIds(),
            config.mutateProperty()
        );
    }
}
