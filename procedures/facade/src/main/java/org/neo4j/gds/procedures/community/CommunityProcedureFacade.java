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
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
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
import org.neo4j.gds.procedures.algorithms.community.KmeansStatsResult;
import org.neo4j.gds.procedures.algorithms.community.ProcedureStatisticsComputationInstructions;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
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
