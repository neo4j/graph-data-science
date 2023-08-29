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
import org.neo4j.gds.algorithms.ComputationResultForStream;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.WccSpecificFields;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

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
        ProgressTracker progressTracker,
        boolean computeComponentCount,
        boolean computeComponentDistribution
    ) {

        // 1. Run the algorithm and time the execution
        var computeMilliseconds = new AtomicLong();
        AlgorithmComputationResult<WccMutateConfig, DisjointSetStruct> algorithmResult;
        try (var ignored = ProgressTimer.start(computeMilliseconds::set)) {
            algorithmResult = this.communityAlgorithmsFacade.wcc(
                graphName,
                config,
                user,
                databaseId
            );
        } catch (Exception e) {
            log.warn("Computation failed", e);
            progressTracker.endSubTaskWithFailure();
            throw e;
        }

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
        var addNodePropertyResult = GraphStoreUpdater.addNodeProperty(
            algorithmResult.graph(),
            algorithmResult.graphStore(),
            config.nodeLabelIdentifiers(algorithmResult.graphStore()),
            config.mutateProperty(),
            nodePropertyValues,
            this.log
        );

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
            .computeMillis(computeMilliseconds.get())
            .postProcessingMillis(communityStatistics.computeMilliseconds())
            .nodePropertiesWritten(addNodePropertyResult.nodePropertiesAdded())
            .mutateMillis(addNodePropertyResult.mutateMilliseconds())
            .configuration(config)
            .algorithmSpecificFields(new WccSpecificFields(componentCount, communitySummary))
            .build();

    }

    public AlgorithmComputationResult<KCoreDecompositionBaseConfig, KCoreDecompositionResult> kCore(
        String graphName,
        KCoreDecompositionBaseConfig config,
        User user,
        DatabaseId databaseId
    ) {
        return this.communityAlgorithmsFacade.kCore(
            graphName,
            config,
            user,
            databaseId
        );
    }

}
