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
package org.neo4j.gds.procedures.algorithms.community.mutate;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.community.SccMutateStep;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.community.CommunityDistributionHelpers;
import org.neo4j.gds.procedures.algorithms.community.SccMutateResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public class SccMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<HugeLongArray>, Stream<SccMutateResult>> {

    private final Map<String, Object> configuration;
    private final boolean consecutiveIds;
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;


    public SccMutateResultTransformer(
        Map<String, Object> configuration,
        boolean consecutiveIds,
        StatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        Graph graph,
        GraphStore graphStore
    ) {
        this.configuration = configuration;
        this.consecutiveIds = consecutiveIds;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
    }

    @Override
    public Stream<SccMutateResult> apply(TimedAlgorithmResult<HugeLongArray> timedAlgorithmResult) {

        var sccResult = timedAlgorithmResult.result();

        var mutateStep = new SccMutateStep(
            mutateNodePropertyService,
            labelsToUpdate,
            mutateProperty,
            consecutiveIds
        );

        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            sccResult
        );

        var nodeCount =sccResult.size();
        var communityStatisticsWithTiming = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            sccResult::get,
            statisticsComputationInstructions
        );

        var statistics = communityStatisticsWithTiming.statistics();

        var sccMutate =
            new SccMutateResult(
                statistics.componentCount(),
                communityStatisticsWithTiming.distribution(),
                0,
                timedAlgorithmResult.computeMillis(),
                statistics.computeMilliseconds(),
                mutateMetadata.mutateMillis(),
                mutateMetadata.nodePropertiesWritten().value(),
                configuration
            );

        return Stream.of(sccMutate);
    }

}
