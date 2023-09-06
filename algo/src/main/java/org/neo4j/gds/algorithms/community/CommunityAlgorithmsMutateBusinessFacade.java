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
import org.neo4j.gds.algorithms.CommunityStatisticsSpecificFields;
import org.neo4j.gds.algorithms.KCoreSpecificFields;
import org.neo4j.gds.algorithms.LouvainSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StandardCommunityStatisticsSpecificFields;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.scc.SccMutateConfig;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;

public class CommunityAlgorithmsMutateBusinessFacade {

    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;
    private final NodePropertyService nodePropertyService;

    public CommunityAlgorithmsMutateBusinessFacade(
        CommunityAlgorithmsFacade communityAlgorithmsFacade,
        NodePropertyService nodePropertyService
    ) {
        this.nodePropertyService = nodePropertyService;
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public NodePropertyMutateResult<StandardCommunityStatisticsSpecificFields> mutateWcc(
        String graphName,
        WccMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeComponentCount,
        boolean computeComponentDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.wcc(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            (result, config) -> CommunityResultCompanion.nodePropertyValues(
                config.isIncremental(),
                config.consecutiveIds(),
                result.asNodeProperties()
            ),
            (result -> result::setIdOf),
            (result, componentCount, communitySummary) -> {
                return new StandardCommunityStatisticsSpecificFields(
                    componentCount,
                    communitySummary
                );
            },
            computeComponentCount,
            computeComponentDistribution,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY
        );
    }


    public NodePropertyMutateResult<KCoreSpecificFields> mutateKCore(
        String graphName,
        KCoreDecompositionMutateConfig config,
        User user,
        DatabaseId databaseId
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.kCore(graphName, config, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            config,
            (result, configuration) -> NodePropertyValuesAdapter.adapt(result.coreValues()),
            (result) -> new KCoreSpecificFields(result.degeneracy()),
            intermediateResult.computeMilliseconds,
            () -> new KCoreSpecificFields(0)
        );
    }

    public NodePropertyMutateResult<LouvainSpecificFields> mutateLouvain(
        String graphName,
        LouvainMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeComponentCount,
        boolean computeComponentDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.louvain(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        NodePropertyValuesMapper<LouvainResult, LouvainMutateConfig> mapper = ((result, config) -> {
            return config.includeIntermediateCommunities()
                ? louvainIntermediateCommunitiesNodePropertyValues(result)
                : CommunityResultCompanion.nodePropertyValues(
                    config.isIncremental(),
                    config.consecutiveIds(),
                    NodePropertyValuesAdapter.adapt(result.dendrogramManager().getCurrent())
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
            computeComponentCount,
            computeComponentDistribution,
            intermediateResult.computeMilliseconds,
            () -> LouvainSpecificFields.EMPTY
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
        boolean computeComponentCount,
        boolean computeComponentDistribution,
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
            var addNodePropertyResult = nodePropertyService.mutate(
                configuration.mutateProperty(), nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()), algorithmResult.graph(),
                algorithmResult.graphStore()
            );

            // 4. Compute result statistics
            var communityStatistics = CommunityStatistics.communityStats(
                nodePropertyValues.nodeCount(),
                communityFunctionSupplier.communityFunction(result),
                Pools.DEFAULT,
                configuration.concurrency(),
                computeComponentCount,
                computeComponentDistribution
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

    public NodePropertyMutateResult<StandardCommunityStatisticsSpecificFields> mutateScc(
        String graphName,
        SccMutateConfig configuration,
        User user,
        DatabaseId databaseId,
        boolean computeComponentCount,
        boolean computeComponentDistribution
    ) {
        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.scc(graphName, configuration, user, databaseId)
        );
        var algorithmResult = intermediateResult.algorithmResult;

        return mutateNodeProperty(
            algorithmResult,
            configuration,
            (result, config) -> NodePropertyValuesAdapter.adapt(result),
            (result -> result::get),
            (result, componentCount, communitySummary) -> {
                return new StandardCommunityStatisticsSpecificFields(
                    componentCount,
                    communitySummary
                );
            },
            computeComponentCount,
            computeComponentDistribution,
            intermediateResult.computeMilliseconds,
            () -> StandardCommunityStatisticsSpecificFields.EMPTY
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
            var addNodePropertyResult = nodePropertyService.mutate(
                configuration.mutateProperty(),
                nodePropertyValues,
                configuration.nodeLabelIdentifiers(algorithmResult.graphStore()),
                algorithmResult.graph(), algorithmResult.graphStore()
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

    private <T> AlgorithmResultWithTiming<T> runWithTiming(Supplier<T> function) {

        var computeMilliseconds = new AtomicLong();
        T algorithmResult;
        try (var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            algorithmResult = function.get();
        }

        return new AlgorithmResultWithTiming<>(algorithmResult, computeMilliseconds.get());
    }

    private static LongArrayNodePropertyValues louvainIntermediateCommunitiesNodePropertyValues(LouvainResult result) {
        var size = result.size();
        return new LongArrayNodePropertyValues() {
            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public long[] longArrayValue(long nodeId) {
                return result.getIntermediateCommunities(nodeId);
            }
        };
    }

    private static final class AlgorithmResultWithTiming<T> {
        final T algorithmResult;
        final long computeMilliseconds;

        private AlgorithmResultWithTiming(
            T algorithmResult,
            long computeMilliseconds
        ) {
            this.computeMilliseconds = computeMilliseconds;
            this.algorithmResult = algorithmResult;
        }
    }

    // Herein lie some private functional interfaces, so we know what we're doing 🤨
    interface NodePropertyValuesMapper<R, C extends MutateNodePropertyConfig> {
        NodePropertyValues map(R result, C configuration);
    }

    interface CommunityFunctionSupplier<R> {
        LongUnaryOperator communityFunction(R result);
    }

    interface SpecificFieldsWithCommunityStatisticsSupplier<R, ASF> {
        ASF specificFields(R result, long componentCount, Map<String, Object> communitySummary);
    }

    interface SpecificFieldsSupplier<R, ASF> {
        ASF specificFields(R result);
    }

}
