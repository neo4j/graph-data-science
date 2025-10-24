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
package org.neo4j.gds.procedures.algorithms.community.stats;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.procedures.algorithms.community.KmeansStatsResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class KMeansStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<KmeansResult>, Stream<KmeansStatsResult>> {

    private final Map<String, Object> configuration;
    private final KmeansStatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;

    public KMeansStatsResultTransformer(
        Map<String, Object> configuration,
        KmeansStatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<KmeansStatsResult> apply(TimedAlgorithmResult<KmeansResult> timedAlgorithmResult) {

        var kmeansResult = timedAlgorithmResult.result();

        var nodePropertyValues = NodePropertyValuesAdapter.adapt(kmeansResult.communities());


        var  distribution = CommunityDistributionHelpers.compute(
            nodePropertyValues,
            concurrency,
            nodeId -> kmeansResult.communities().get(nodeId),
            statisticsComputationInstructions
        );

        var centroids = computeCentroids(
            statisticsComputationInstructions.shouldComputeListOfCentroids(),
            kmeansResult.centers()
        );

        var kmeansStatsResult = new KmeansStatsResult(
            0,
            timedAlgorithmResult.computeMillis(),
            distribution.statistics().computeMilliseconds(),
            distribution.summary(),
            centroids,
            kmeansResult.averageDistanceToCentroid(),
            kmeansResult.averageSilhouette(),
            configuration
        );

        return Stream.of(kmeansStatsResult);

    }

    private List<List<Double>> computeCentroids(boolean shouldComputeListOfCentroids, double[][] matrix) {
        if (shouldComputeListOfCentroids) {
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

    record KmeansStatisticsComputationInstructions(boolean computeCountOnly, boolean computeCountAndDistribution, boolean shouldComputeListOfCentroids) implements  StatisticsComputationInstructions{

        static KmeansStatisticsComputationInstructions create(ProcedureReturnColumns procedureReturnColumns){
                var intermediate = ProcedureStatisticsComputationInstructions.forCommunities(procedureReturnColumns);
                return new KmeansStatisticsComputationInstructions(intermediate.computeCountOnly(),
                    intermediate.computeCountAndDistribution(),
                    procedureReturnColumns.contains("centroids")
                );
        }
    }
}
