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
import org.neo4j.gds.community.KMeansMutateStep;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.procedures.algorithms.MutateNodeStepExecute;
import org.neo4j.gds.procedures.algorithms.community.CommunityDistributionHelpers;
import org.neo4j.gds.procedures.algorithms.community.KMeansMutateResult;
import org.neo4j.gds.procedures.algorithms.community.KmeansStatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.procedures.algorithms.community.KmeansConvenienceMethods.computeCentroids;

public class KMeansMutateResultTransformer implements ResultTransformer<TimedAlgorithmResult<KmeansResult>, Stream<KMeansMutateResult>> {

    private final Map<String, Object> configuration;
    private final KmeansStatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final Collection<String> labelsToUpdate;
    private final String mutateProperty;
    private final Graph graph;
    private final GraphStore graphStore;

    public KMeansMutateResultTransformer(
        Map<String, Object> configuration,
        KmeansStatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency,
        MutateNodePropertyService mutateNodePropertyService,
        Collection<String> labelsToUpdate,
        String mutateProperty,
        Graph graph,
        GraphStore graphStore
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.labelsToUpdate = labelsToUpdate;
        this.mutateProperty = mutateProperty;
        this.graph = graph;
        this.graphStore = graphStore;
    }

    @Override
    public Stream<KMeansMutateResult> apply(TimedAlgorithmResult<KmeansResult> timedAlgorithmResult) {

        var kmeansResult = timedAlgorithmResult.result();

        var nodeCount = kmeansResult.communities().size();

        var distribution = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            nodeId -> kmeansResult.communities().get(nodeId),
            statisticsComputationInstructions
        );

        var centroids = computeCentroids(
            statisticsComputationInstructions.shouldComputeListOfCentroids(),
            kmeansResult.centers()
        );

        var mutateStep = new KMeansMutateStep(mutateNodePropertyService,labelsToUpdate,mutateProperty);
        var mutateMetadata = MutateNodeStepExecute.executeMutateNodePropertyStep(
            mutateStep,
            graph,
            graphStore,
            kmeansResult
        );

        var kmeansMutateResult = new KMeansMutateResult(
            0,
            timedAlgorithmResult.computeMillis(),
            distribution.statistics().computeMilliseconds(),
            mutateMetadata.mutateMillis(),
            mutateMetadata.nodePropertiesWritten().value(),
            distribution.distribution(),
            centroids,
            kmeansResult.averageDistanceToCentroid(),
            kmeansResult.averageSilhouette(),
            configuration
        );

        return Stream.of(kmeansMutateResult);

    }



}
