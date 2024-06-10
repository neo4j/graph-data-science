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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.community.CommunityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringStatsConfig;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringWriteConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStatsConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionWriteConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.kmeans.KmeansStatsConfig;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.kmeans.KmeansWriteConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationWriteConfig;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenStatsConfig;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.leiden.LeidenWriteConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainStatsConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.louvain.LouvainWriteConfig;
import org.neo4j.gds.modularity.ModularityStatsConfig;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMutateConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStatsConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationWriteConfig;
import org.neo4j.gds.procedures.algorithms.community.ProcedureStatisticsComputationInstructions;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutMutateResult;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutStreamResult;
import org.neo4j.gds.procedures.community.conductance.ConductanceStreamResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringMutateResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringStatsResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringStreamResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringWriteResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionMutateResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionStatsResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionStreamResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionWriteResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansMutateResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansStatsResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansStreamResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansWriteResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationMutateResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStatsResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStreamResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationWriteResult;
import org.neo4j.gds.procedures.community.leiden.LeidenMutateResult;
import org.neo4j.gds.procedures.community.leiden.LeidenStatsResult;
import org.neo4j.gds.procedures.community.leiden.LeidenStreamResult;
import org.neo4j.gds.procedures.community.leiden.LeidenWriteResult;
import org.neo4j.gds.procedures.community.louvain.LouvainMutateResult;
import org.neo4j.gds.procedures.community.louvain.LouvainStatsResult;
import org.neo4j.gds.procedures.community.louvain.LouvainStreamResult;
import org.neo4j.gds.procedures.community.louvain.LouvainWriteResult;
import org.neo4j.gds.procedures.community.modularity.ModularityStatsResult;
import org.neo4j.gds.procedures.community.modularity.ModularityStreamResult;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationMutateResult;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationStatsResult;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationStreamResult;
import org.neo4j.gds.procedures.community.modularityoptimization.ModularityOptimizationWriteResult;
import org.neo4j.gds.procedures.community.scc.AlphaSccWriteResult;
import org.neo4j.gds.procedures.community.scc.SccMutateResult;
import org.neo4j.gds.procedures.community.scc.SccStatsResult;
import org.neo4j.gds.procedures.community.scc.SccStreamResult;
import org.neo4j.gds.procedures.community.scc.SccWriteResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientMutateResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientStatsResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientStreamResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientWriteResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountMutateResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStatsResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStreamResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountWriteResult;
import org.neo4j.gds.procedures.algorithms.community.WccStatsResult;
import org.neo4j.gds.procedures.community.wcc.WccStreamResult;
import org.neo4j.gds.procedures.community.wcc.WccWriteResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.scc.SccAlphaWriteConfig;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.scc.SccWriteConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStatsConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.triangle.TriangleCountStatsConfig;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.triangle.TriangleCountWriteConfig;
import org.neo4j.gds.wcc.WccStatsConfig;
import org.neo4j.gds.wcc.WccStreamConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public class CommunityProcedureFacade {
    // services
    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;

    // business logic
    private final CommunityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CommunityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    public CommunityProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        CommunityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CommunityAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
    }

    // WCC

    public Stream<WccStreamResult> wccStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, WccStreamConfig::of);

        var computationResult = streamBusinessFacade.wcc(
            graphName,
            streamConfig
        );

        return WccComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<WccStatsResult> wccStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, WccStatsConfig::of);

        var computationResult = statsBusinessFacade.wcc(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(WccComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<WccWriteResult> wccWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, WccWriteConfig::of);

        var computationResult = writeBusinessFacade.wcc(
            graphName,
            writeConfig,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(WccComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> wccEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, WccStatsConfig::of);
        return Stream.of(estimateBusinessFacade.wcc(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> wccEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, WccWriteConfig::of);
        return Stream.of(estimateBusinessFacade.wcc(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> wccEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, WccStreamConfig::of);
        return Stream.of(estimateBusinessFacade.wcc(graphNameOrConfiguration, config));
    }

    // WCC end

    // K-Core Decomposition
    public Stream<KCoreDecompositionStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(
            configuration,
            KCoreDecompositionStreamConfig::of
        );

        var computationResult = streamBusinessFacade.kCore(
            graphName,
            streamConfig
        );

        return KCoreComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KCoreDecompositionMutateResult> kCoreMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, KCoreDecompositionMutateConfig::of);

        var computationResult = mutateBusinessFacade.kCore(
            graphName,
            config
        );

        return Stream.of(KCoreComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<KCoreDecompositionStatsResult> kCoreStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, KCoreDecompositionStatsConfig::of);

        var computationResult = statsBusinessFacade.kCore(
            graphName,
            config
        );

        return Stream.of(KCoreComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<KCoreDecompositionWriteResult> kCoreWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, KCoreDecompositionWriteConfig::of);

        var computationResult = writeBusinessFacade.kcore(
            graphName,
            config
        );

        return Stream.of(KCoreComputationalResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> kCoreEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KCoreDecompositionStreamConfig::of);
        return Stream.of(estimateBusinessFacade.kcore(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> kCoreEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KCoreDecompositionMutateConfig::of);
        return Stream.of(estimateBusinessFacade.kcore(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> kCoreEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KCoreDecompositionStatsConfig::of);
        return Stream.of(estimateBusinessFacade.kcore(graphNameOrConfiguration, config));
    }


    // K-Core Decomposition end

    public Stream<LouvainStatsResult> louvainStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LouvainStatsConfig::of);

        var computationResult = statsBusinessFacade.louvain(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LouvainComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> louvainEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LouvainStatsConfig::of);
        return Stream.of(estimateBusinessFacade.louvain(graphNameOrConfiguration, config));
    }

    public Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration

    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, LouvainStreamConfig::of);

        var computationResult = streamBusinessFacade.louvain(
            graphName,
            streamConfig
        );

        return LouvainComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<MemoryEstimateResult> louvainEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LouvainStreamConfig::of);
        return Stream.of(estimateBusinessFacade.louvain(graphNameOrConfiguration, config));
    }

    public Stream<LouvainMutateResult> louvainMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LouvainMutateConfig::of);

        var computationResult = mutateBusinessFacade.louvain(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LouvainComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<MemoryEstimateResult> louvainEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LouvainMutateConfig::of);
        return Stream.of(estimateBusinessFacade.louvain(graphNameOrConfiguration, config));
    }

    public Stream<LouvainWriteResult> louvainWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LouvainWriteConfig::of);

        var computationResult = writeBusinessFacade.louvain(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LouvainComputationResultTransformer.toWriteResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> louvainEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LouvainWriteConfig::of);
        return Stream.of(estimateBusinessFacade.louvain(graphNameOrConfiguration, config));
    }

    public Stream<LeidenStreamResult> leidenStream(
        String graphName,
        Map<String, Object> configuration

    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, LeidenStreamConfig::of);

        var computationResult = streamBusinessFacade.leiden(
            graphName,
            streamConfig
        );

        return LeidenComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<LeidenMutateResult> leidenMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LeidenMutateConfig::of);

        var computationResult = mutateBusinessFacade.leiden(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LeidenComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<LeidenStatsResult> leidenStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LeidenStatsConfig::of);

        var computationResult = statsBusinessFacade.leiden(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LeidenComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<LeidenWriteResult> leidenWrite(String graphName, Map<String, Object> configuration) {
        var config = configurationCreator.createConfiguration(configuration, LeidenWriteConfig::of);

        var computationResult = writeBusinessFacade.leiden(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LeidenComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> leidenEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LeidenStreamConfig::of);
        return Stream.of(estimateBusinessFacade.leiden(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> leidenEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LeidenMutateConfig::of);
        return Stream.of(estimateBusinessFacade.leiden(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> leidenEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LeidenStatsConfig::of);
        return Stream.of(estimateBusinessFacade.leiden(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> leidenEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LeidenWriteConfig::of);
        return Stream.of(estimateBusinessFacade.leiden(graphNameOrConfiguration, config));
    }

    public Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration

    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, SccStreamConfig::of);

        var computationResult = streamBusinessFacade.scc(
            graphName,
            streamConfig
        );

        return SccComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<SccMutateResult> sccMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, SccMutateConfig::of);

        var computationResult = mutateBusinessFacade.scc(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<SccWriteResult> sccWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, SccWriteConfig::of);

        var computationResult = writeBusinessFacade.scc(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<SccStatsResult> sccStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, SccStatsConfig::of);

        var computationResult = statsBusinessFacade.scc(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> sccEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, SccWriteConfig::of);
        return Stream.of(estimateBusinessFacade.estimateScc(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> sccEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, SccMutateConfig::of);
        return Stream.of(estimateBusinessFacade.estimateScc(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> sccEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, SccStatsConfig::of);
        return Stream.of(estimateBusinessFacade.estimateScc(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> sccEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, SccStreamConfig::of);
        return Stream.of(estimateBusinessFacade.estimateScc(graphNameOrConfiguration, config));
    }


    public Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration

    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, TriangleCountStreamConfig::of);

        var computationResult = streamBusinessFacade.triangleCount(
            graphName,
            streamConfig
        );

        return TriangleCountComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<TriangleCountMutateResult> triangleCountMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, TriangleCountMutateConfig::of);

        var computationResult = mutateBusinessFacade.triangleCount(
            graphName,
            config
        );

        return Stream.of(TriangleCountComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<TriangleCountStatsResult> triangleCountStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, TriangleCountStatsConfig::of);

        var computationResult = statsBusinessFacade.triangleCount(
            graphName,
            config
        );

        return Stream.of(TriangleCountComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<TriangleCountWriteResult> triangleCountWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, TriangleCountWriteConfig::of);

        var computationResult = writeBusinessFacade.triangleCount(
            graphName,
            config
        );

        return Stream.of(TriangleCountComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> triangleCountEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, TriangleCountStreamConfig::of);
        return Stream.of(estimateBusinessFacade.triangleCount(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> triangleCountEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, TriangleCountMutateConfig::of);
        return Stream.of(estimateBusinessFacade.triangleCount(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> triangleCountEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, TriangleCountStatsConfig::of);
        return Stream.of(estimateBusinessFacade.triangleCount(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> triangleCountEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, TriangleCountWriteConfig::of);
        return Stream.of(estimateBusinessFacade.triangleCount(graphNameOrConfiguration, config));
    }

    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, LabelPropagationStreamConfig::of);

        var computationResult = streamBusinessFacade.labelPropagation(
            graphName,
            streamConfig
        );

        return LabelPropagationComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<LabelPropagationMutateResult> labelPropagationMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, LabelPropagationMutateConfig::of);

        var computationResult = mutateBusinessFacade.labelPropagation(
            graphName,
            mutateConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<LabelPropagationStatsResult> labelPropagationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LabelPropagationStatsConfig::of);

        var computationResult = statsBusinessFacade.labelPropagation(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<LabelPropagationWriteResult> labelPropagationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, LabelPropagationWriteConfig::of);

        var computationResult = writeBusinessFacade.labelPropagation(
            graphName,
            config,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> labelPropagationEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LabelPropagationStreamConfig::of);
        return Stream.of(estimateBusinessFacade.labelPropagation(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> labelPropagationEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LabelPropagationStatsConfig::of);
        return Stream.of(estimateBusinessFacade.labelPropagation(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> labelPropagationEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LabelPropagationMutateConfig::of);
        return Stream.of(estimateBusinessFacade.labelPropagation(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> labelPropagationEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LabelPropagationWriteConfig::of);
        return Stream.of(estimateBusinessFacade.labelPropagation(graphNameOrConfiguration, config));
    }

    public Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration

    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, ModularityStreamConfig::of);

        var computationResult = streamBusinessFacade.modularity(
            graphName,
            streamConfig
        );

        return ModularityComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<ModularityStatsResult> modularityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, ModularityStatsConfig::of);

        var computationResult = statsBusinessFacade.modularity(
            graphName,
            config
        );

        return Stream.of(ModularityComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<MemoryEstimateResult> modularityEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityStreamConfig::of);
        return Stream.of(estimateBusinessFacade.modularity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> modularityEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityStatsConfig::of);
        return Stream.of(estimateBusinessFacade.modularity(graphNameOrConfiguration, config));
    }

    public Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, KmeansStreamConfig::of);

        var computationResult = streamBusinessFacade.kmeans(
            graphName,
            streamConfig
        );

        return KmeansComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KmeansMutateResult> kmeansMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, KmeansMutateConfig::of);

        var computationResult = mutateBusinessFacade.kmeans(
            graphName,
            mutateConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns),
            procedureReturnColumns.contains("centroids")
        );

        return Stream.of(KmeansComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<KmeansWriteResult> kmeansWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, KmeansWriteConfig::of);

        var computationResult = writeBusinessFacade.kmeans(
            graphName,
            writeConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns),
            procedureReturnColumns.contains("centroids")
        );

        return Stream.of(KmeansComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<KmeansStatsResult> kmeansStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, KmeansStatsConfig::of);

        var computationResult = statsBusinessFacade.kmeans(
            graphName,
            statsConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns),
            procedureReturnColumns.contains("centroids")
        );

        return Stream.of(KmeansComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<MemoryEstimateResult> kmeansEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KmeansMutateConfig::of);
        return Stream.of(estimateBusinessFacade.kmeans(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> kmeansEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KmeansStatsConfig::of);
        return Stream.of(estimateBusinessFacade.kmeans(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> kmeansEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KmeansStreamConfig::of);
        return Stream.of(estimateBusinessFacade.kmeans(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> kmeansEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KmeansWriteConfig::of);
        return Stream.of(estimateBusinessFacade.kmeans(graphNameOrConfiguration, config));
    }

    public Stream<LocalClusteringCoefficientStreamResult> localClusteringCoefficientStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(
            configuration,
            LocalClusteringCoefficientStreamConfig::of
        );

        var computationResult = streamBusinessFacade.localClusteringCoefficient(
            graphName,
            streamConfig
        );

        return LCCComputationResultTransformer.toStreamResult(computationResult);
    }


    public Stream<LocalClusteringCoefficientMutateResult> localClusteringCoefficientMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, LocalClusteringCoefficientMutateConfig::of);

        var computationResult = mutateBusinessFacade.localClusteringCoefficient(
            graphName,
            mutateConfig
        );

        return Stream.of(LCCComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<LocalClusteringCoefficientStatsResult> localClusteringCoefficientStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, LocalClusteringCoefficientStatsConfig::of);

        var computationResult = statsBusinessFacade.localClusteringCoefficient(
            graphName,
            statsConfig
        );

        return Stream.of(LCCComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<LocalClusteringCoefficientWriteResult> localClusteringCoefficientWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, LocalClusteringCoefficientWriteConfig::of);

        var computationResult = writeBusinessFacade.localClusteringCoefficient(
            graphName,
            writeConfig
        );

        return Stream.of(LCCComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientMutateConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientWriteConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientStatsConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientStreamConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }


    public Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(
            configuration,
            K1ColoringStreamConfig::of
        );

        var computationResult = streamBusinessFacade.k1coloring(
            graphName,
            streamConfig
        );

        return K1ColoringComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<K1ColoringMutateResult> k1ColoringMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, K1ColoringMutateConfig::of);

        var computationResult = mutateBusinessFacade.k1coloring(
            graphName,
            mutateConfig,
            procedureReturnColumns.contains("colorCount")
        );

        return Stream.of(K1ColoringComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<K1ColoringWriteResult> k1ColoringWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, K1ColoringWriteConfig::of);

        var computationResult = writeBusinessFacade.k1coloring(
            graphName,
            writeConfig,
            procedureReturnColumns.contains("colorCount")
        );

        return Stream.of(K1ColoringComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<ModularityOptimizationStreamResult> modularityOptimizationStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(
            configuration,
            ModularityOptimizationStreamConfig::of
        );

        var computationResult = streamBusinessFacade.modularityOptimization(
            graphName,
            streamConfig
        );

        return ModularityOptimisationComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<ModularityOptimizationMutateResult> modularityOptimizationMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, ModularityOptimizationMutateConfig::of);

        var computationResult = mutateBusinessFacade.modularityOptimization(
            graphName,
            mutateConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(ModularityOptimisationComputationResultTransformer.toMutateResult(
            computationResult,
            mutateConfig
        ));
    }

    public Stream<ModularityOptimizationStatsResult> modularityOptimizationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, ModularityOptimizationStatsConfig::of);

        var computationResult = statsBusinessFacade.modularityOptimization(
            graphName,
            statsConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(ModularityOptimisationComputationResultTransformer.toStatsResult(
            computationResult,
            statsConfig
        ));
    }

    public Stream<ModularityOptimizationWriteResult> modularityOptimizationWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, ModularityOptimizationWriteConfig::of);

        var computationResult = writeBusinessFacade.modularityOptimization(
            graphName,
            writeConfig,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(ModularityOptimisationComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<MemoryEstimateResult> modularityOptimizationEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityOptimizationStreamConfig::of);
        return Stream.of(estimateBusinessFacade.modularityOptimization(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> modularityOptimizationEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityOptimizationStatsConfig::of);
        return Stream.of(estimateBusinessFacade.modularityOptimization(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> modularityOptimizationEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityOptimizationMutateConfig::of);
        return Stream.of(estimateBusinessFacade.modularityOptimization(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> modularityOptimizationEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ModularityOptimizationWriteConfig::of);
        return Stream.of(estimateBusinessFacade.modularityOptimization(graphNameOrConfiguration, config));
    }


    public Stream<K1ColoringStatsResult> k1ColoringStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, K1ColoringStatsConfig::of);

        var computationResult = statsBusinessFacade.k1coloring(
            graphName,
            statsConfig,
            procedureReturnColumns.contains("colorCount")
        );

        return Stream.of(K1ColoringComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<MemoryEstimateResult> k1ColoringEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, K1ColoringMutateConfig::of);
        return Stream.of(estimateBusinessFacade.k1Coloring(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> k1ColoringEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, K1ColoringStatsConfig::of);
        return Stream.of(estimateBusinessFacade.k1Coloring(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> k1ColoringEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, K1ColoringStreamConfig::of);
        return Stream.of(estimateBusinessFacade.k1Coloring(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> k1ColoringEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, K1ColoringWriteConfig::of);
        return Stream.of(estimateBusinessFacade.k1Coloring(graphNameOrConfiguration, config));
    }

    public Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(
            configuration,
            ConductanceStreamConfig::of
        );

        var computationResult = streamBusinessFacade.conductance(
            graphName,
            streamConfig
        );

        return ConductanceComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, ApproxMaxKCutStreamConfig::of);

        var computationResult = streamBusinessFacade.approxMaxKCut(
            graphName,
            streamConfig
        );

        return ApproxMaxKCutComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<ApproxMaxKCutMutateResult> approxMaxKCutMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfiguration(configuration, ApproxMaxKCutMutateConfig::of);

        var computationResult = mutateBusinessFacade.approxMaxKCut(
            graphName,
            streamConfig
        );

        return Stream.of(ApproxMaxKCutComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<MemoryEstimateResult> approxMaxKCutEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ApproxMaxKCutStreamConfig::of);
        return Stream.of(estimateBusinessFacade.approxMaxKCut(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> approxMaxKCutEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, ApproxMaxKCutMutateConfig::of);
        return Stream.of(estimateBusinessFacade.approxMaxKCut(graphNameOrConfiguration, config));
    }

    public Stream<AlphaSccWriteResult> alphaSccWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationCreator.createConfiguration(configuration, SccAlphaWriteConfig::of);

        var computationResult = writeBusinessFacade.alphaScc(
            graphName,
            config,
            new ProcedureStatisticsComputationInstructions(true, true)
        );

        return Stream.of(SccComputationResultTransformer.toAlphaWriteResult(computationResult, config));
    }
}
