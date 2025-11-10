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
import org.neo4j.gds.community.LabelPropagationMutateStep;
import org.neo4j.gds.community.StandardCommunityProperties;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.community.CommunityDistributionHelpers;
import org.neo4j.gds.procedures.algorithms.community.LabelPropagationMutateResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

public class LabelPropagationMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<LabelPropagationResult>, Stream<LabelPropagationMutateResult>> {

    private final Map<String, Object> configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;
    private final StandardCommunityProperties standardCommunityProperties;

    public LabelPropagationMutateResultTransformer(
        Map<String, Object> configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        Graph graph,
        GraphStore graphStore,
        StandardCommunityProperties standardCommunityProperties
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
        this.standardCommunityProperties = standardCommunityProperties;
    }

    @Override
    public Stream<LabelPropagationMutateResult> apply(TimedAlgorithmResult<LabelPropagationResult> timedAlgorithmResult) {

        var labelPropagationResult = timedAlgorithmResult.result();
        var nodeCount = labelPropagationResult.labels().size();
        var labels =  labelPropagationResult.labels();

        var mutateStep = new LabelPropagationMutateStep(
            mutateNodePropertyService,
            labelsToUpdate,
            mutateProperty,
            standardCommunityProperties
        );
        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            labelPropagationResult
        );

        var communityStatisticsWithTiming = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            labels::get,
            statisticsComputationInstructions
        );
        var statistics = communityStatisticsWithTiming.statistics();

        var labelPropagationMutateResult = new LabelPropagationMutateResult(
            labelPropagationResult.ranIterations(),
            labelPropagationResult.didConverge(),
            statistics.componentCount(),
            communityStatisticsWithTiming.distribution(),
            0,
            timedAlgorithmResult.computeMillis(),
            statistics.computeMilliseconds(),
            mutateMetadata.mutateMillis(),
            mutateMetadata.nodePropertiesWritten().value(),
            configuration
        );

        return Stream.of(labelPropagationMutateResult);

    }

}
