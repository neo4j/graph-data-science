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
import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.community.specificfields.AlphaSccSpecificFields;
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
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ArrowConnectionInfo;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.k1coloring.K1ColoringWriteConfig;
import org.neo4j.gds.kcore.KCoreDecompositionWriteConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansWriteConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationWriteConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenWriteConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainWriteConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationWriteConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccAlphaWriteConfig;
import org.neo4j.gds.scc.SccWriteConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientWriteConfig;
import org.neo4j.gds.triangle.TriangleCountWriteConfig;
import org.neo4j.gds.wcc.WccWriteConfig;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.gds.algorithms.community.CommunityCompanion.arrayMatrixToListMatrix;
import static org.neo4j.gds.algorithms.community.CommunityCompanion.createIntermediateCommunitiesNodePropertyValues;
import static org.neo4j.gds.algorithms.runner.AlgorithmRunner.runWithTiming;

public class CommunityAlgorithmsWriteBusinessFacade {

    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;
    private final WriteNodePropertyService writeNodePropertyService;

    public CommunityAlgorithmsWriteBusinessFacade(
        WriteNodePropertyService writeNodePropertyService,
        CommunityAlgorithmsFacade communityAlgorithmsFacade
    ) {
        this.writeNodePropertyService = writeNodePropertyService;
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public NodePropertyWriteResult<StandardCommunityStatisticsSpecificFields> wcc(
        String graphName,
        WccWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.wcc(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            configuration,
            (result, config) -> CommunityCompanion.nodePropertyValues(
                config.isIncremental(),
                config.seedProperty(),
                config.writeProperty(),
                config.consecutiveIds(),
                result.asNodeProperties(),
                config.minCommunitySize(),
                config.typedConcurrency(),
                () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
            ),
            (result -> result::setIdOf),
            (result, componentCount, communitySummary) -> new StandardCommunityStatisticsSpecificFields(
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY,
            "WccWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );

    }

    public NodePropertyWriteResult<KCoreSpecificFields> kcore(
        String graphName,
        KCoreDecompositionWriteConfig configuration
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.kCore(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            configuration,
            (result, config) -> NodePropertyValuesAdapter.adapt(result.coreValues()),
            (result) -> new KCoreSpecificFields(result.degeneracy()),
            intermediateResult.computeMilliseconds,
            () -> KCoreSpecificFields.EMPTY,
            "KCoreWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );

    }

    public NodePropertyWriteResult<StandardCommunityStatisticsSpecificFields> scc(
        String graphName,
        SccWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.scc(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            configuration,
            (result, config) -> CommunityCompanion.nodePropertyValues(
                config.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result),
                Optional.empty(),
                config.typedConcurrency()
            ),
            (result -> result::get),
            (result, componentCount, communitySummary) -> new StandardCommunityStatisticsSpecificFields(
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY,
            "SccWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );

    }

    public NodePropertyWriteResult<AlphaSccSpecificFields> alphaScc(
        String graphName,
        SccAlphaWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.scc(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            configuration,
            (result, config) -> NodePropertyValuesAdapter.adapt(result),
            (result -> result::get),
            (result, componentCount, communitySummary) -> new AlphaSccSpecificFields(
                result.size(),
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> AlphaSccSpecificFields.EMPTY,
            "SccWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );

    }

    public NodePropertyWriteResult<LouvainSpecificFields> louvain(
        String graphName,
        LouvainWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.louvain(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LouvainResult, LouvainWriteConfig> mapper = ((result, config) -> config.includeIntermediateCommunities()
            ? createIntermediateCommunitiesNodePropertyValues(result::getIntermediateCommunities, result.size())
            : CommunityCompanion.nodePropertyValues(
                config.isIncremental(),
                config.writeProperty(),
                config.seedProperty(),
                config.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
                config.minCommunitySize(),
                config.typedConcurrency(),
                () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
            ));

        return writeToDatabase(
            algorithmResult,
            configuration,
            mapper,
            (result -> result::getCommunity),
            (result, componentCount, communitySummary) -> new LouvainSpecificFields(
                result.modularity(),
                Arrays.stream(result.modularities()).boxed().collect(Collectors.toList()),
                result.ranLevels(),
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LouvainSpecificFields.EMPTY,
            "LouvainWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<LabelPropagationSpecificFields>  labelPropagation(
        String graphName,
        LabelPropagationWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.labelPropagation(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            configuration,
            ((result, config) -> CommunityCompanion.nodePropertyValues(
                config.isIncremental(),
                config.writeProperty(),
                config.seedProperty(),
                config.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result.labels()),
                config.minCommunitySize(),
                config.typedConcurrency(),
                () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
            )),
            (result -> result.labels()::get),
            (result, componentCount, communitySummary) -> LabelPropagationSpecificFields.from(
                result.ranIterations(),
                result.didConverge(),
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LabelPropagationSpecificFields.EMPTY,
            "LabelPropagationWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<LeidenSpecificFields> leiden(
        String graphName,
        LeidenWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = AlgorithmRunner.runWithTiming(
            () -> communityAlgorithmsFacade.leiden(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LeidenResult, LeidenWriteConfig> mapper = ((result, config) -> config.includeIntermediateCommunities()
            ? createIntermediateCommunitiesNodePropertyValues(
            result::getIntermediateCommunities,
            result.communities().size()
        )
            : CommunityCompanion.nodePropertyValues(
                config.isIncremental(),
                config.writeProperty(),
                config.seedProperty(),
                config.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent()),
                config.minCommunitySize(),
                config.typedConcurrency(),
                () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
            ));

        return writeToDatabase(
            algorithmResult,
            configuration,
            mapper,
            (result -> result.communities()::get),
            (result, componentCount, communitySummary) -> LeidenSpecificFields.from(
                result.communities().size(),
                result.modularity(),
                result.modularities(),
                componentCount,
                result.ranLevels(),
                result.didConverge(),
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> LeidenSpecificFields.EMPTY,
            "LeidenWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );
    }


    public NodePropertyWriteResult<KmeansSpecificFields> kmeans(
        String graphName,
        KmeansWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        boolean computeListOfCentroids
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.kmeans(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<KmeansResult, KmeansWriteConfig> mapper = ((result, config) ->
            NodePropertyValuesAdapter.adapt(result.communities()));

        return writeToDatabase(
            algorithmResult,
            configuration,
            mapper,
            (result -> result.communities()::get),
            (result, componentCount, communitySummary) -> new KmeansSpecificFields(
                communitySummary,
                arrayMatrixToListMatrix(computeListOfCentroids, result.centers()),
                result.averageDistanceToCentroid(),
                result.averageSilhouette()
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            () -> KmeansSpecificFields.EMPTY,
            "KmeansWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<K1ColoringSpecificFields> k1coloring(
        String graphName,
        K1ColoringWriteConfig config,
        boolean computeUsedColors
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.k1Coloring(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            config,
            (result, configuration) -> CommunityCompanion.nodePropertyValues(
                false,
                NodePropertyValuesAdapter.adapt(result.colors()),
                configuration.minCommunitySize(),
                configuration.typedConcurrency()
            ),
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
            () -> K1ColoringSpecificFields.EMPTY,
            "K1ColoringWrite",
            config.typedWriteConcurrency(),
            config.writeProperty(),
            config.arrowConnectionInfo(),
            config.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<ModularityOptimizationSpecificFields> modularityOptimization(
        String graphName,
        ModularityOptimizationWriteConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.modularityOptimization(graphName, configuration)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        Supplier<ModularityOptimizationSpecificFields> emptySupplier = () -> ModularityOptimizationSpecificFields.EMPTY;

        return writeToDatabase(
            algorithmResult,
            configuration,
            (result, config) -> CommunityCompanion.nodePropertyValues(
                config.isIncremental(),
                config.writeProperty(),
                config.seedProperty(),
                config.consecutiveIds(),
                result.asNodeProperties(),
                config.minCommunitySize(),
                config.typedConcurrency(),
                () -> algorithmResult.graphStore().nodeProperty(config.seedProperty())
            ),
            result -> result::communityId,
            (result, componentCount, communitySummary) -> new ModularityOptimizationSpecificFields(
                result.modularity(),
                result.ranIterations(),
                result.didConverge(),
                result.asNodeProperties().nodeCount(),
                componentCount,
                communitySummary
            ),
            statisticsComputationInstructions,
            intermediateResult.computeMilliseconds,
            emptySupplier,
            "ModularityOptimizationWrite",
            configuration.typedWriteConcurrency(),
            configuration.writeProperty(),
            configuration.arrowConnectionInfo(),
            configuration.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<TriangleCountSpecificFields> triangleCount(
        String graphName,
        TriangleCountWriteConfig config
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.triangleCount(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.localTriangles()),
            (result) -> new TriangleCountSpecificFields(result.globalTriangles(), algorithmResult.graph().nodeCount()),
            intermediateResult.computeMilliseconds,
            () -> TriangleCountSpecificFields.EMPTY,
            "TriangleCountWrite",
            config.typedWriteConcurrency(),
            config.writeProperty(),
            config.arrowConnectionInfo(),
            config.resolveResultStore(algorithmResult.resultStore())
        );
    }

    public NodePropertyWriteResult<LocalClusteringCoefficientSpecificFields> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientWriteConfig config
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.localClusteringCoefficient(graphName, config)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return writeToDatabase(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.localClusteringCoefficients()),
            (result) -> new LocalClusteringCoefficientSpecificFields(
                result.localClusteringCoefficients().size(),
                result.averageClusteringCoefficient()
            ),
            intermediateResult.computeMilliseconds,
            () -> LocalClusteringCoefficientSpecificFields.EMPTY,
            "LocalClusteringCoefficientWrite",
            config.typedWriteConcurrency(),
            config.writeProperty(),
            config.arrowConnectionInfo(),
            config.resolveResultStore(algorithmResult.resultStore())
        );
    }


    <RESULT, CONFIG extends AlgoBaseConfig, ASF extends CommunityStatisticsSpecificFields> NodePropertyWriteResult<ASF> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        NodePropertyValuesMapper<RESULT, CONFIG> nodePropertyValuesMapper,
        CommunityFunctionSupplier<RESULT> communityFunctionSupplier,
        SpecificFieldsWithCommunityStatisticsSupplier<RESULT, ASF> specificFieldsSupplier,
        StatisticsComputationInstructions statisticsComputationInstructions,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        String procedureName,
        Concurrency writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore
    ) {

        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result,
                configuration
            );

            // 3. Write to database
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                algorithmResult.graphStore(),
                nodePropertyValues,
                writeConcurrency,
                writeProperty,
                procedureName,
                arrowConnectionInfo,
                resultStore
            );

            // 4. Compute result statistics
            var communityStatistics = CommunityStatistics.communityStats(
                nodePropertyValues.nodeCount(),
                communityFunctionSupplier.communityFunction(result),
                DefaultPool.INSTANCE,
                configuration.typedConcurrency(),
                statisticsComputationInstructions
            );

            var componentCount = communityStatistics.componentCount();
            var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

            var specificFields = specificFieldsSupplier.specificFields(result, componentCount, communitySummary);

            return NodePropertyWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(communityStatistics.computeMilliseconds())
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(emptyASFSupplier.get(), configuration));

    }

    <RESULT, CONFIG extends AlgoBaseConfig, ASF> NodePropertyWriteResult<ASF> writeToDatabase(
        AlgorithmComputationResult<RESULT> algorithmResult,
        CONFIG configuration,
        NodePropertyValuesMapper<RESULT, CONFIG> nodePropertyValuesMapper,
        SpecificFieldsSupplier<RESULT, ASF> specificFieldsSupplier,
        long computeMilliseconds,
        Supplier<ASF> emptyASFSupplier,
        String procedureName,
        Concurrency writeConcurrency,
        String writeProperty,
        Optional<ArrowConnectionInfo> arrowConnectionInfo,
        Optional<ResultStore> resultStore
    ) {

        return algorithmResult.result().map(result -> {
            // 2. Construct NodePropertyValues from the algorithm result
            // 2.1 Should we measure some post-processing here?
            var nodePropertyValues = nodePropertyValuesMapper.map(
                result,
                configuration
            );

            // 3. Write to database
            var writeNodePropertyResult = writeNodePropertyService.write(
                algorithmResult.graph(),
                algorithmResult.graphStore(),
                nodePropertyValues,
                writeConcurrency,
                writeProperty,
                procedureName,
                arrowConnectionInfo,
                resultStore
            );


            var specificFields = specificFieldsSupplier.specificFields(result);

            return NodePropertyWriteResult.<ASF>builder()
                .computeMillis(computeMilliseconds)
                .postProcessingMillis(0)
                .nodePropertiesWritten(writeNodePropertyResult.nodePropertiesWritten())
                .writeMillis(writeNodePropertyResult.writeMilliseconds())
                .configuration(configuration)
                .algorithmSpecificFields(specificFields)
                .build();
        }).orElseGet(() -> NodePropertyWriteResult.empty(emptyASFSupplier.get(), configuration));

    }

}
