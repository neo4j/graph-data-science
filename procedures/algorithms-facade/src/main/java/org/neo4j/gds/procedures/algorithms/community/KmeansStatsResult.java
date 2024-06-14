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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.List;
import java.util.Map;

public class KmeansStatsResult extends StandardStatsResult {
    public final Map<String, Object> communityDistribution;
    public final List<List<Double>> centroids;
    public final double averageDistanceToCentroid;
    public final double averageSilhouette;

    public KmeansStatsResult(
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> communityDistribution,
        List<List<Double>> centroids,
        double averageDistanceToCentroid,
        double averageSilhouette,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.communityDistribution = communityDistribution;
        this.centroids = centroids;
        this.averageDistanceToCentroid = averageDistanceToCentroid;
        this.averageSilhouette = averageSilhouette;
    }

    public static final class Builder extends AbstractCommunityResultBuilder<KmeansStatsResult> {
        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        private List<List<Double>> centroids;
        private double averageDistanceToCentroid;
        private double averageSilhouette;
        @Override
        public KmeansStatsResult buildResult() {
            return new KmeansStatsResult(
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                communityHistogramOrNull(),
                centroids,
                averageDistanceToCentroid,
                averageSilhouette,
                config.toMap()
            );
        }

        public Builder withCentroids(List<List<Double>> listCenters) {
            this.centroids = listCenters;
            return this;
        }

        public Builder withAverageDistanceToCentroid(double averageDistanceToCentroid) {
            this.averageDistanceToCentroid = averageDistanceToCentroid;
            return this;
        }

        public Builder withAverageSilhouette(double averageSilhouette) {
            this.averageSilhouette = averageSilhouette;
            return this;
        }
    }
}
