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
package org.neo4j.gds.kmeans;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

import java.util.List;
import java.util.Map;

public class MutateResult extends StatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    public MutateResult(
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        @Nullable Map<String, Object> communityDistribution,
        @Nullable List<List<Double>> centroids,
        @Nullable double averageDistanceToCentroid,
        @Nullable double averageSilhouette,
        Map<String, Object> configuration
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            communityDistribution,
            centroids,
            averageDistanceToCentroid,
            averageSilhouette,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static class Builder extends AbstractCommunityResultBuilder<MutateResult> {
        private List<List<Double>> centroids;
        private double averageDistanceToCentroid;

        private double averageSilhouette;
        Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected MutateResult buildResult() {
            return new MutateResult(
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                mutateMillis,
                nodePropertiesWritten,
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
