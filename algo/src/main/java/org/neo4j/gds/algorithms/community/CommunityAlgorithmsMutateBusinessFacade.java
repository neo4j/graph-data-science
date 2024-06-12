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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.community.specificfields.CommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.K1ColoringSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.KCoreSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.KmeansSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.LabelPropagationSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.LeidenSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.LocalClusteringCoefficientSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.LouvainSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.ModularityOptimizationSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.community.specificfields.TriangleCountSpecificFields;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.k1coloring.K1ColoringMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kmeans.KmeansMutateConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationMutateConfig;
import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMutateConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMutateConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.TriangleCountMutateConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.neo4j.gds.algorithms.community.CommunityCompanion.createIntermediateCommunitiesNodePropertyValues;
import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CommunityAlgorithmsMutateBusinessFacade {

    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;
    private final MutateNodePropertyService mutateNodePropertyService;

    public CommunityAlgorithmsMutateBusinessFacade(
        CommunityAlgorithmsFacade communityAlgorithmsFacade,
        MutateNodePropertyService mutateNodePropertyService
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public NodePropertyMutateResult<KCoreSpecificFields> kCore(
        String graphName,
        KCoreDecompositionMutateConfig config
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.kCore(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.coreValues()),
            (result) -> new KCoreSpecificFields(result.degeneracy()),
            intermediateResult.computeMilliseconds,
            () -> KCoreSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<LouvainSpecificFields> louvain(
        String graphName,
        LouvainMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.louvain(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LouvainResult, LouvainMutateConfig> mapper = ((result, config) -> {
            return config.includeIntermediateCommunities()
                ? createIntermediateCommunitiesNodePropertyValues(result::getIntermediateCommunities, result.size())
                : CommunityCompanion.nodePropertyValues(
                    config.isIncremental(),
                    config.mutateProperty(),
                    config.seedProperty(),
                    config.consecutiveIds(),
                    NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
                    () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
                );
        });

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            mapper,
            (result -> result::getCommunity),
            (result, componentCount, communitySummary) -> {
                return LouvainSpecificFields.from(
                    result.modularity(),
                    result.modularities(),
                    componentCount,
                    result.ranLevels(),
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LouvainSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<LeidenSpecificFields> leiden(
        String graphName,
        LeidenMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.leiden(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LeidenResult, LeidenMutateConfig> mapper = ((result, config) -> {
            return config.includeIntermediateCommunities()
                ? createIntermediateCommunitiesNodePropertyValues(
                result::getIntermediateCommunities,
                result.communities().size()
            )
                : CommunityCompanion.nodePropertyValues(
                    config.isIncremental(),
                    config.mutateProperty(),
                    config.seedProperty(),
                    config.consecutiveIds(),
                    NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
                    () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
                );
        });

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            mapper,
            (result -> result.communities()::get),
            (result, componentCount, communitySummary) -> {
                return LeidenSpecificFields.from(
                    result.communities().size(),
                    result.modularity(),
                    result.modularities(),
                    componentCount,
                    result.ranLevels(),
                    result.didConverge(),
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LeidenSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<StandardCommunityStatisticsSpecificFields> scc(
        String graphName,
        SccMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.scc(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            (result, config) -> CommunityCompanion.nodePropertyValues(
                config.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result)
            ),
            (result -> result::get),
            (result, componentCount, communitySummary) -> {
                return new StandardCommunityStatisticsSpecificFields(
                    componentCount,
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY
        );

    }

    public NodePropertyMutateResult<LabelPropagationSpecificFields> labelPropagation(
        String graphName,
        LabelPropagationMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.labelPropagation(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            ((result1, config) -> {
                return CommunityCompanion.nodePropertyValues(
                    config.isIncremental(),
                    config.mutateProperty(),
                    config.seedProperty(),
                    config.consecutiveIds(),
                    NodePropertyValuesAdapter.adapt(result1.labels()),
                    () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
                );
            }),
            (result -> result.labels()::get),
            (result, componentCount, communitySummary) -> {
                return LabelPropagationSpecificFields.from(
                    result.ranIterations(),
                    result.didConverge(),
                    componentCount,
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LabelPropagationSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<TriangleCountSpecificFields> triangleCount(
        String graphName,
        TriangleCountMutateConfig config
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.triangleCount(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.localTriangles()),
            (result) -> new TriangleCountSpecificFields(result.globalTriangles(), algorithmResult.graph().nodeCount()),
            intermediateResult.computeMilliseconds,
            () -> TriangleCountSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<K1ColoringSpecificFields> k1coloring(
        String graphName,
        K1ColoringMutateConfig config,
        boolean computeUsedColors
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.k1Coloring(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.colors()),
            (result) -> {
                long usedColors = (computeUsedColors) ? result.usedColors().cardinality() : 0;

                return new K1ColoringSpecificFields(
                    result.colors().size(),
                    usedColors,
                    result.ranIterations(),
                    result.didConverge()
                );
            },
            intermediateResult.computeMilliseconds,
            () -> K1ColoringSpecificFields.EMPTY
        );
    }


    /*
        By using `ASF extends CommunityStatisticsSpecificFields` we enforce the algorithm specific fields
        to contain the statistics information.
     */
    <RESULT, CONFIG extends MutateNodePropertyConfig, ASF extends CommunityStatisticsSpecificFields> NodePropertyMutateResult<ASF> mutateNodeProperty(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        NodePropertyValuesMapper<RESULT, CONFIG> nodePropertyValuesMapper,
        CommunityFunctionSupplier<RESULT> communityFunctionSupplier,
        SpecificFieldsWithCommunityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        StatisticsComputationInstructions statisticsComputationInstructions,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {

        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result,
                configuration
            );

            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(),
                algorithmResult.graphStore()
            );

            // 4. Compute result statistics
            var communityStatistics = CommunityStatistics.communityStats(
                nodePropertyValues.nodeCount(),
                communityFunctionSupplier.communityFunction(result),
                DefaultPool.INSTANCE,
                configuration.concurrency(),
                statisticsComputationInstructions
            );

            var componentCount = communityStatistics.componentCount();
            var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, componentCount, communitySummary);

            return NodePropertyMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(communityStatistics.computeMilliseconds())
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(emptyASFSupplier.get(), configuration));

    }

    public NodePropertyMutateResult<KmeansSpecificFields> kmeans(
        String graphName,
        KmeansMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        boolean computeListOfCentroids
        ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.kmeans(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<KmeansResult, KmeansMutateConfig> mapper = ((result, config) ->
            NodePropertyValuesAdapter.adapt(result.communities()));

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            mapper,
            (result -> result.communities()::get),
            (result, componentCount, communitySummary) -> {
                return new KmeansSpecificFields(
                    communitySummary,
                    arrayMatrixToListMatrix(computeListOfCentroids, result.centers()),
                    result.averageDistanceToCentroid(),
                    result.averageSilhouette()
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> KmeansSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<LocalClusteringCoefficientSpecificFields> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientMutateConfig configuration
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.localClusteringCoefficient(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LocalClusteringCoefficientResult, LocalClusteringCoefficientMutateConfig> mapper = ((result, config) ->
            NodePropertyValuesAdapter.adapt(result.localClusteringCoefficients()));


        return mutateNodeProperty(
            algorithmResult,
            configuration,
            mapper,
            (result) -> new LocalClusteringCoefficientSpecificFields(
                result.localClusteringCoefficients().size(),
                result.averageClusteringCoefficient()
            ),
            intermediateResult.computeMilliseconds,
            () -> LocalClusteringCoefficientSpecificFields.EMPTY
        );
    }

    public NodePropertyMutateResult<ModularityOptimizationSpecificFields> modularityOptimization(
        String graphName,
        ModularityOptimizationMutateConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.modularityOptimization(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            ((modularityOptimizationResult, config) -> {
                return CommunityCompanion.nodePropertyValues(
                    config.isIncremental(),
                    config.mutateProperty(),
                    config.seedProperty(),
                    config.consecutiveIds(),
                    modularityOptimizationResult.asNodeProperties(),
                    () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
                );
            }),
            (result -> result::communityId),
            (result, componentCount, communitySummary) -> {
                return new ModularityOptimizationSpecificFields(
                    result.modularity(),
                    result.ranIterations(),
                    result.didConverge(),
                    result.asNodeProperties().nodeCount(),
                    componentCount,
                    communitySummary
                );
            },
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> ModularityOptimizationSpecificFields.EMPTY
        );
    }

    <RESULT, CONFIG extends MutateNodePropertyConfig, ASF> NodePropertyMutateResult<ASF> mutateNodeProperty(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        NodePropertyValuesMapper<RESULT, CONFIG> nodePropertyValuesMapper,
        SpecificFieldsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier
    ) {
        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result,
                configuration
            );

            // 3. Go and mutate the graph store
            var addNodePropertyResult = mutateNodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(),
                algorithmResult.graphStore()
            );

            var specificFields = specificFieldsSupplier.specificFields(result);

            return NodePropertyMutateResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
                .mutateMillis(addNodePropertyResult.mutateMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyMutateResult.empty(emptyASFSupplier.get(), configuration));

    }

    private List<List<Double>> arrayMatrixToListMatrix(boolean shouldCompute, double[][] matrix) {
        if (shouldCompute) {
            var result = new ArrayList<List<Double>>();

            for (double[] row : matrix) {
                List<Double> rowList = new ArrayList<>();
                result.add(rowList);
                for (double column : row)
                    rowList.add(column);
            }
            return result;
        }
        return null;
    }

}
