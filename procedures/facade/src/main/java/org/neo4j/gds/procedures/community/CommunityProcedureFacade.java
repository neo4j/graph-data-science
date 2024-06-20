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
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientStreamResult;
import org.neo4j.gds.procedures.community.triangle.LocalClusteringCoefficientWriteResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountMutateResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStatsResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountStreamResult;
import org.neo4j.gds.procedures.community.triangleCount.TriangleCountWriteResult;
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

    // business logic
    private final CommunityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CommunityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    public CommunityProcedureFacade(
        ConfigurationCreator configurationCreator,
        CommunityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        CommunityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CommunityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        CommunityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CommunityAlgorithmsWriteBusinessFacade writeBusinessFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
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

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientWriteConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> localClusteringCoefficientEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, LocalClusteringCoefficientStreamConfig::of);
        return Stream.of(estimateBusinessFacade.localClusteringCoefficient(graphNameOrConfiguration, config));
    }
}
