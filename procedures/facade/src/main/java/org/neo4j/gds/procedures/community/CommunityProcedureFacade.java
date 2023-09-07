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
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.kmeans.KmeansStreamConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.modularity.ModularityStreamConfig;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionMutateResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansMutateResult;
import org.neo4j.gds.procedures.community.kmeans.KmeansStreamResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationMutateResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStreamResult;
import org.neo4j.gds.procedures.community.louvain.LouvainMutateResult;
import org.neo4j.gds.procedures.community.louvain.LouvainStreamResult;
import org.neo4j.gds.procedures.community.modularity.ModularityStreamResult;
import org.neo4j.gds.procedures.community.scc.SccMutateResult;
import org.neo4j.gds.procedures.community.scc.SccStreamResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountMutateResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStreamResult;
import org.neo4j.gds.procedures.community.wcc.WccMutateResult;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.scc.SccStreamConfig;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;
import org.neo4j.gds.triangle.TriangleCountStreamConfig;
import org.neo4j.gds.wcc.WccMutateConfig;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class CommunityProcedureFacade {
    private final CommunityAlgorithmsStreamBusinessFacade algorithmsStreamBusinessFacade;
    private final CommunityAlgorithmsMutateBusinessFacade algorithmsMutateBusinessFacade;

    private final ProcedureReturnColumns procedureReturnColumns;
    private final DatabaseId databaseId;
    private final User user;

    public CommunityProcedureFacade(
        CommunityAlgorithmsStreamBusinessFacade algorithmsStreamBusinessFacade,
        CommunityAlgorithmsMutateBusinessFacade algorithmsMutateBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns,
        DatabaseId databaseId,
        User user
    ) {
        this.algorithmsStreamBusinessFacade = algorithmsStreamBusinessFacade;
        this.algorithmsMutateBusinessFacade = algorithmsMutateBusinessFacade;
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

        var computationResult = algorithmsStreamBusinessFacade.wcc(
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
        var config = createMutateConfig(configuration, WccMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.wcc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(WccComputationResultTransformer.toMutateResult(computationResult));
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

        var computationResult = algorithmsStreamBusinessFacade.kCore(
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
        var config = createMutateConfig(configuration, KCoreDecompositionMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.kCore(
            graphName,
            config,
            user,
            databaseId
        );

        return Stream.of(KCoreComputationalResultTransformer.toMutateResult(computationResult));
    }
    // K-Core Decomposition end

    public Stream<LouvainStreamResult> louvainStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, LouvainStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = algorithmsStreamBusinessFacade.louvain(
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
        var config = createMutateConfig(configuration, LouvainMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.louvain(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LouvainComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<SccStreamResult> sccStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, SccStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = algorithmsStreamBusinessFacade.scc(
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
        var config = createMutateConfig(configuration, SccMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.scc(
            graphName,
            config,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forComponents(procedureReturnColumns)
        );

        return Stream.of(SccComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<TriangleCountStreamResult> triangleCountStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, TriangleCountStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = algorithmsStreamBusinessFacade.triangleCount(
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
        var config = createMutateConfig(configuration, TriangleCountMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.triangleCount(
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

        var computationResult = algorithmsStreamBusinessFacade.labelPropagation(
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
        var mutateConfig = createMutateConfig(configuration, LabelPropagationMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.labelPropagation(
            graphName,
            mutateConfig,
            user,
            databaseId,
            ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns)
        );

        return Stream.of(LabelPropagationComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<ModularityStreamResult> modularityStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter

    ) {
        var streamConfig = createStreamConfig(configuration, ModularityStreamConfig::of, algorithmMetaDataSetter);

        var computationResult = algorithmsStreamBusinessFacade.modularity(
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

        var computationResult = algorithmsStreamBusinessFacade.kmeans(
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
        var mutateConfig = createMutateConfig(configuration, KmeansMutateConfig::of);

        var computationResult = algorithmsMutateBusinessFacade.kmeans(
            graphName,
            mutateConfig,
            user,
            databaseId,
            procedureReturnColumns.contains("communityDistribution"),
            procedureReturnColumns.contains("centroids")
        );

        return Stream.of(KmeansComputationResultTransformer.toMutateResult(computationResult));
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

    private <C extends AlgoBaseConfig> C createMutateConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return configCreator.apply(CypherMapWrapper.create(configuration));
    }

}
