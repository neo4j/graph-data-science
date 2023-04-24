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
package org.neo4j.gds.triangle;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;

@SuppressWarnings("unused")
public class LocalClusteringCoefficientStatsResult extends StandardStatsResult {

    public final double averageClusteringCoefficient;
    public final long nodeCount;

    LocalClusteringCoefficientStatsResult(
        double averageClusteringCoefficient,
        long nodeCount,
        long preProcessingMillis,
        long computeMillis,
        Map<String, Object> configuration
    ) {
        // post-processing is instant for TC
        super(preProcessingMillis, computeMillis, 0L, configuration);
        this.averageClusteringCoefficient = averageClusteringCoefficient;
        this.nodeCount = nodeCount;
    }

    static class Builder extends AbstractResultBuilder<LocalClusteringCoefficientStatsResult> {

        double averageClusteringCoefficient = 0;

        Builder withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }

        @Override
        public LocalClusteringCoefficientStatsResult build() {
            return new LocalClusteringCoefficientStatsResult(
                averageClusteringCoefficient,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                config.toMap()
            );
        }
    }
}
