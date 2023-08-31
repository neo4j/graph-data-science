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

import org.apache.commons.math3.util.Pair;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.ComputationResultForStream;
import org.neo4j.gds.algorithms.KCoreSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.WccSpecificFields;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMutateConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class CommunityAlgorithmsBusinessFacade {
    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;
    private final Log log;


    public CommunityAlgorithmsBusinessFacade(CommunityAlgorithmsFacade communityAlgorithmsFacade, Log log) {
        this.log = log;
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public ComputationResultForStream<WccBaseConfig, DisjointSetStruct> streamWcc(
        String graphName,
        WccBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        var wccResult = this.communityAlgorithmsFacade.wcc(
            graphName,
            config,
            user,
            databaseId
        );

        return ComputationResultForStream.of(
            wccResult.result(),
            wccResult.configuration(),
            wccResult.graph(),
            wccResult.graphStore()
        );
    }

    public NodePropertyMutateResult<WccSpecificFields> mutateWcc(
        String graphName,
        WccMutateConfig config,
        User user,
        DatabaseId databaseId,
        boolean computeComponentCount,
        boolean computeComponentDistribution
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.wcc(graphName, config, user, databaseId)
        );
        var algorithmResult = intermediateResult.getSecond();

        // 2. Construct NodePropertyValues from the algorithm result
        // 2.1 Should we measure some post-processing here?
        var nodePropertyValues = CommunityResultCompanion.nodePropertyValues(
            config.isIncremental(),
            config.consecutiveIds(),
            algorithmResult.result()
                .map(DisjointSetStruct::asNodeProperties)
                .orElse(EmptyLongNodePropertyValues.INSTANCE),
            Optional.empty(),
            config.concurrency()
        );

        // 3. Go and mutate the graph store
        var addNodePropertyResult = mutateNodeProperty(nodePropertyValues, config, algorithmResult);

        // 4. Compute result statistics
        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertyValues.nodeCount(),
            nodePropertyValues::longValue,
            Pools.DEFAULT,
            config.concurrency(),
            computeComponentCount,
            computeComponentDistribution
        );

        var componentCount = communityStatistics.componentCount();
        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

        return NodePropertyMutateResult.<WccSpecificFields>builder()
            .computeMillis(intermediateResult.getFirst().get())
            .postProcessingMillis(communityStatistics.computeMilliseconds())
            .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
            .mutateMillis(addNodePropertyResult.mutateMilliseconds())
            .configuration(config)
            .algorithmSpecificFields(new WccSpecificFields(componentCount, communitySummary))
            .build();

    }

    public ComputationResultForStream<KCoreDecompositionBaseConfig, KCoreDecompositionResult> streamKCore(
        String graphName,
        KCoreDecompositionBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        var kcoreResult = this.communityAlgorithmsFacade.kCore(
            graphName,
            config,
            user,
            databaseId
        );

        return ComputationResultForStream.of(
            kcoreResult.result(),
            kcoreResult.configuration(),
            kcoreResult.graph(),
            kcoreResult.graphStore()
        );
    }

    public NodePropertyMutateResult<KCoreSpecificFields> mutateΚcore(
        String graphName,
        KCoreDecompositionMutateConfig config,
        User user,
        DatabaseId databaseId
    ) {

        // 1. Run the algorithm and time the execution
        var intermediateResult = runWithTiming(
            () -> communityAlgorithmsFacade.kCore(graphName, config, user, databaseId)
        );
        var algorithmResult = intermediateResult.getSecond();

        var nodePropertyValues = algorithmResult.result()
            .map(result -> NodePropertyValuesAdapter.adapt(result.coreValues()))
            .orElseGet(() -> EmptyLongNodePropertyValues.INSTANCE);

        // 3. Go and mutate the graph store
        var addNodePropertyResult = mutateNodeProperty(nodePropertyValues, config, algorithmResult);

        return NodePropertyMutateResult.<KCoreSpecificFields>builder()
            .computeMillis(intermediateResult.getFirst().get())
            .postProcessingMillis(0L)
            .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
            .mutateMillis(addNodePropertyResult.mutateMilliseconds())
            .configuration(config)
            .algorithmSpecificFields(new KCoreSpecificFields(algorithmResult.result()
                .map(KCoreDecompositionResult::degeneracy)
                .orElse(0)))
            .build();

    }

    private <C extends AlgoBaseConfig, T> Pair<AtomicLong, T> runWithTiming(Supplier<T> function) {

        var computeMilliseconds = new AtomicLong();
        T algorithmResult;
        try (var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            algorithmResult = function.get();
        }

        return new Pair<>(computeMilliseconds, algorithmResult);
    }

    private <C extends MutateNodePropertyConfig, T> AddNodePropertyResult mutateNodeProperty(
        NodePropertyValues nodePropertyValues,
        C config,
        AlgorithmComputationResult<C, T> algorithmResult
    ) {
        return GraphStoreUpdater.addNodeProperty(
            algorithmResult.graph(),
            algorithmResult.graphStore(),
            config.nodeLabelIdentifiers(algorithmResult.graphStore()),
            config.mutateProperty(),
            nodePropertyValues,
            this.log
        );
    }

}
