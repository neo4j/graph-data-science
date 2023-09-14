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

import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStatsConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutMutateResult;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutStreamResult;
import org.neo4j.gds.procedures.community.conductance.ConductanceStreamResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringMutateResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringStreamResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionMutateResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionStatsResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansMutateResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansStreamResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationMutateResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStatsResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStreamResult;
import org.neo4j.gds.procedures.community.leiden.LeidenMutateResult;
import org.neo4j.gds.procedures.community.leiden.LeidenStreamResult;
import org.neo4j.gds.procedures.community.louvain.LouvainMutateResult;
import org.neo4j.gds.procedures.community.louvain.LouvainStreamResult;
import org.neo4j.gds.procedures.community.modularity.ModularityStreamResult;
import org.neo4j.gds.procedures.community.scc.SccMutateResult;
import org.neo4j.gds.procedures.community.scc.SccStatsResult;
import org.neo4j.gds.procedures.community.scc.SccStreamResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientMutateResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientStreamResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountMutateResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStreamResult;
import org.neo4j.gds.procedures.community.wcc.WccMutateResult;
import org.neo4j.gds.procedures.community.wcc.WccStatsResult;
import org.neo4j.gds.procedures.community.wcc.WccStreamResult;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.scc.SccStatsConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.wcc.WccMutateConfig;
import org.neo4j.gds.wcc.WccStatsConfig;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class CommunityProcedureFacade {
    private final CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade;

    private final ProcedureReturnColumns procedureReturnColumns;
    private final DatabaseId databaseId;
    private final User user;

    public CommunityProcedureFacade(
        CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns,
        DatabaseId databaseId,
        User user
    ) {
        this.streamBusinessFacade = streamBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.procedureReturnColumns = procedureReturnColumns;
        this.databaseId = databaseId;
        this.user = user;
    }

    // WCC

    public Stream<WccStreamResult> wccStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(configuration, WccStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.wcc(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return WccComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<WccMutateResult> wccMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, WccMutateConfig::of);

        var computationResult = mutateBusinessFacade.wcc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(WccComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<WccStatsResult> wccStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, WccStatsConfig::of);

        var computationResult = statsBusinessFacade.wcc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(WccComputationResultTransformer.toStatsResult(computationResult, config));
    }

    // WCC end

    // K-Core Decomposition
    public Stream<KCoreStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(
            configuration,
            KCoreDecompositionStreamConfig::of,
            algorithmMetaDataSetter
        );

        var computationResult = streamBusinessFacade.kCore(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return KCoreComputationalResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KCoreDecompositionMutateResult> kCoreMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, KCoreDecompositionMutateConfig::of);

        var computationResult = mutateBusinessFacade.kCore(
            graphName,
            config,
            user,
            databaseId
        );

        return Stream.of(KCoreComputationalResultTransformer.toMutateResult(computationResult));
    }

    public Stream<KCoreDecompositionStatsResult> kCoreStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, KCoreDecompositionStatsConfig::of);

        var computationResult = statsBusinessFacade.kCore(
            graphName,
            config,
            user,
            databaseId
        );

        return Stream.of(KCoreComputationalResultTransformer.toStatsResult(computationResult, config));
    }

    // K-Core Decomposition end

    public Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, LouvainStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.louvain(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return LouvainComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<LouvainMutateResult> louvainMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, LouvainMutateConfig::of);

        var computationResult = mutateBusinessFacade.louvain(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LouvainComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<LeidenStreamResult> leidenStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, LeidenStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.leiden(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return LeidenComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<LeidenMutateResult> leidenMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, LeidenMutateConfig::of);

        var computationResult = mutateBusinessFacade.leiden(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LeidenComputationResultTransformer.toMutateResult(computationResult));
    }


    public Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, SccStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.scc(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return SccComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<SccMutateResult> sccMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, SccMutateConfig::of);

        var computationResult = mutateBusinessFacade.scc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<SccStatsResult> sccStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, SccStatsConfig::of);

        var computationResult = statsBusinessFacade.scc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, TriangleCountStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.triangleCount(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return TriangleCountComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<TriangleCountMutateResult> triangleCountMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, TriangleCountMutateConfig::of);

        var computationResult = mutateBusinessFacade.triangleCount(
            graphName,
            config,
            user,
            databaseId
        );

        return Stream.of(TriangleCountComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<LabelPropagationStreamResult> labelPropagationStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(configuration, LabelPropagationStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.labelPropagation(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return LabelPropagationComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<LabelPropagationMutateResult> labelPropagationMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = createConfig(configuration, LabelPropagationMutateConfig::of);

        var computationResult = mutateBusinessFacade.labelPropagation(
            graphName,
            mutateConfig,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<LabelPropagationStatsResult> labelPropagationStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = createConfig(configuration, LabelPropagationStatsConfig::of);

        var computationResult = statsBusinessFacade.labelPropagation(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toStatsResult(computationResult, config));
    }

    public Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, ModularityStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.modularity(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return ModularityComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KmeansStreamResult> kmeansStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(configuration, KmeansStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.kmeans(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return KmeansComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KmeansMutateResult> kmeansMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = createConfig(configuration, KmeansMutateConfig::of);

        var computationResult = mutateBusinessFacade.kmeans(
            graphName,
            mutateConfig,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns),
            procedureReturnColumns.contains("centroids")
        );

        return Stream.of(KmeansComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<LocalClusteringCoefficientStreamResult> streamLocalClusteringCoefficient(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(
            configuration,
            LocalClusteringCoefficientStreamConfig::of,
            algorithmMetaDataSetter
        );

        var computationResult = streamBusinessFacade.localClusteringCoefficient(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return LCCComputationResultTransformer.toStreamResult(computationResult);
    }


    public Stream<LocalClusteringCoefficientMutateResult> mutateLocalClusteringCoefficient(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = createConfig(configuration, LocalClusteringCoefficientMutateConfig::of);

        var computationResult = mutateBusinessFacade.localClusteringCoefficient(
            graphName,
            mutateConfig,
            user,
            databaseId
        );

        return Stream.of(LCCComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<K1ColoringStreamResult> k1ColoringStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(
            configuration,
            K1ColoringStreamConfig::of,
            algorithmMetaDataSetter
        );

        var computationResult = streamBusinessFacade.k1coloring(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return K1ColoringComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<K1ColoringMutateResult> k1ColoringMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = createConfig(
            configuration,
            K1ColoringMutateConfig::of
        );

        var computationResult = mutateBusinessFacade.k1coloring(
            graphName,
            mutateConfig,
            user,
            databaseId,
            procedureReturnColumns.contains("colorCount")
        );

        return Stream.of(K1ColoringComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<ConductanceStreamResult> conductanceStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(
            configuration,
            ConductanceStreamConfig::of,
            algorithmMetaDataSetter
        );

        var computationResult = streamBusinessFacade.conductance(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return ConductanceComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCutStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = createStreamConfig(configuration, ApproxMaxKCutStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = streamBusinessFacade.approxMaxKCut(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return ApproxMaxKCutComputationResultTransformer.toStreamResult(computationResult, streamConfig);
    }

    public Stream<ApproxMaxKCutMutateResult> approxMaxKCutMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = createConfig(configuration, ApproxMaxKCutMutateConfig::of);

        var computationResult = mutateBusinessFacade.approxMaxKCut(
            graphName,
            streamConfig,
            user,
            databaseId
        );

        return Stream.of(ApproxMaxKCutComputationResultTransformer.toMutateResult(computationResult));
    }

    private <C extends AlgoBaseConfig> C createStreamConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var config = configCreator.apply(CypherMapWrapper.create(configuration));
        algorithmMetaDataSetter.set(config);
        return config;
    }

    private <C extends AlgoBaseConfig> C createConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return configCreator.apply(CypherMapWrapper.create(configuration));
    }

}
