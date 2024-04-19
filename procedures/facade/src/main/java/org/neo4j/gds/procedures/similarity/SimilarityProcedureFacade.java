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
package org.neo4j.gds.procedures.similarity;

import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.similarity.SimilarityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.similarity.KnnMutateResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityMutateResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStatsResult;
import org.neo4j.gds.procedures.algorithms.similarity.KnnStatsResult;
import org.neo4j.gds.procedures.algorithms.similarity.KnnWriteResult;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityWriteResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnMutateConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStatsConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnWriteConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityWriteConfig;
import org.neo4j.gds.similarity.knn.KnnStatsConfig;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;
import org.neo4j.gds.similarity.knn.KnnWriteConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityMutateConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStatsConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @deprecated use {@link org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade} instead
 */
@Deprecated
public class SimilarityProcedureFacade {
    private final ConfigurationCreator configurationCreator;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final SimilarityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final SimilarityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final SimilarityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final SimilarityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final SimilarityAlgorithmsWriteBusinessFacade writeBusinessFacade;

    /**
     * @deprecated this sits here temporarily
     */
    @Deprecated
    private final org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade theOtherFacade;

    public SimilarityProcedureFacade(
        ConfigurationCreator configurationCreator,
        ProcedureReturnColumns procedureReturnColumns,
        SimilarityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        SimilarityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        SimilarityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        SimilarityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        SimilarityAlgorithmsWriteBusinessFacade writeBusinessFacade,
        org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade theOtherFacade
    ) {
        this.configurationCreator = configurationCreator;
        this.procedureReturnColumns = procedureReturnColumns;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;

        this.theOtherFacade = theOtherFacade;
    }

    public Stream<SimilarityResult> nodeSimilarityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, NodeSimilarityStreamConfig::of);

        var computationResult = streamBusinessFacade.nodeSimilarity(
            graphName,
            streamConfig
        );

        return NodeSimilarityComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<SimilarityStatsResult> nodeSimilarityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, NodeSimilarityStatsConfig::of);

        var computationResult = statsBusinessFacade.nodeSimilarity(
            graphName,
            statsConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<SimilarityWriteResult> nodeSimilarityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, NodeSimilarityWriteConfig::of);

        var computationResult = writeBusinessFacade.nodeSimilarity(
            graphName,
            statsConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toWriteResult(computationResult));
    }

    public Stream<SimilarityMutateResult> nodeSimilarityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, NodeSimilarityMutateConfig::of);

        var computationResult = mutateBusinessFacade.nodeSimilarity(
            graphName,
            mutateConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toMutateResult(computationResult));
    }


    public Stream<MemoryEstimateResult> nodeSimilarityEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, NodeSimilarityStreamConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> nodeSimilarityEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, NodeSimilarityStatsConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> nodeSimilarityEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, NodeSimilarityMutateConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> nodeSimilarityEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, NodeSimilarityWriteConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    //filtered
    public Stream<SimilarityResult> filteredNodeSimilarityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, FilteredNodeSimilarityStreamConfig::of);

        var computationResult = streamBusinessFacade.filteredNodeSimilarity(
            graphName,
            streamConfig
        );

        return NodeSimilarityComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<SimilarityStatsResult> filteredNodeSimilarityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, FilteredNodeSimilarityStatsConfig::of);

        var computationResult = statsBusinessFacade.filteredNodeSimilarity(
            graphName,
            statsConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<SimilarityMutateResult> filteredNodeSimilarityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, FilteredNodeSimilarityMutateConfig::of);

        var computationResult = mutateBusinessFacade.filteredNodeSimilarity(
            graphName,
            mutateConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toMutateResult(computationResult));
    }

    public Stream<SimilarityWriteResult> filteredNodeSimilarityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, FilteredNodeSimilarityWriteConfig::of);

        var computationResult = writeBusinessFacade.filteredNodeSimilarity(
            graphName,
            writeConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(NodeSimilarityComputationResultTransformer.toWriteResult(computationResult));
    }


    public Stream<MemoryEstimateResult> filteredNodeSimilarityEstimateStream(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredNodeSimilarityStreamConfig::of);
        return Stream.of(estimateBusinessFacade.filteredNodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityEstimateStats(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredNodeSimilarityStatsConfig::of);
        return Stream.of(estimateBusinessFacade.filteredNodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityEstimateMutate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredNodeSimilarityMutateConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredNodeSimilarityEstimateWrite(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredNodeSimilarityWriteConfig::of);
        return Stream.of(estimateBusinessFacade.nodeSimilarity(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> knnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KnnStreamConfig::of);
        return Stream.of(estimateBusinessFacade.knn(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> knnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KnnStatsConfig::of);
        return Stream.of(estimateBusinessFacade.knn(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> knnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, KnnWriteConfig::of);
        return Stream.of(estimateBusinessFacade.knn(graphNameOrConfiguration, config));
    }

    public Stream<SimilarityResult> filteredKnnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var streamConfig = configurationCreator.createConfigurationForStream(configuration, FilteredKnnStreamConfig::of);

        var computationResult = streamBusinessFacade.filteredKnn(
            graphName,
            streamConfig
        );

        return FilteredKnnComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<KnnStatsResult> filteredKnnStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var statsConfig = configurationCreator.createConfiguration(configuration, FilteredKnnStatsConfig::of);

        var computationResult = statsBusinessFacade.filteredKnn(
            graphName,
            statsConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(FilteredKnnComputationResultTransformer.toStatsResult(computationResult, statsConfig));
    }

    public Stream<KnnMutateResult> filteredKnnMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        var mutateConfig = configurationCreator.createConfiguration(configuration, FilteredKnnMutateConfig::of);

        var computationResult = mutateBusinessFacade.filteredKnn(
            graphName,
            mutateConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(FilteredKnnComputationResultTransformer.toMutateResult(computationResult, mutateConfig));
    }

    public Stream<KnnWriteResult> filteredKnnWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var writeConfig = configurationCreator.createConfiguration(configuration, FilteredKnnWriteConfig::of);

        var computationResult = writeBusinessFacade.filteredKnn(
            graphName,
            writeConfig,
            procedureReturnColumns.contains("similarityDistribution")
        );

        return Stream.of(FilteredKnnComputationResultTransformer.toWriteResult(computationResult, writeConfig));
    }


    public Stream<MemoryEstimateResult> filteredKnnStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredKnnStreamConfig::of);
        return Stream.of(estimateBusinessFacade.filteredKnn(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredKnnStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredKnnStatsConfig::of);
        return Stream.of(estimateBusinessFacade.filteredKnn(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredKnnMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredKnnMutateConfig::of);
        return Stream.of(estimateBusinessFacade.filteredKnn(graphNameOrConfiguration, config));
    }

    public Stream<MemoryEstimateResult> filteredKnnWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var config = configurationCreator.createConfiguration(algoConfiguration, FilteredKnnWriteConfig::of);
        return Stream.of(estimateBusinessFacade.filteredKnn(graphNameOrConfiguration, config));
    }

    /**
     * @deprecated short term hack while migrating
     */
    @Deprecated
    public org.neo4j.gds.procedures.algorithms.similarity.SimilarityProcedureFacade theOtherFacade() {
        return theOtherFacade;
    }
}
