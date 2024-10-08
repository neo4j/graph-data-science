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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class LocalClusteringCoefficientMutateResult extends LocalClusteringCoefficientStatsResult {
    public long mutateMillis;
    public long nodePropertiesWritten;

    public LocalClusteringCoefficientMutateResult(
        double averageClusteringCoefficient,
        long nodeCount,
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            averageClusteringCoefficient,
            nodeCount,
            preProcessingMillis,
            computeMillis,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LocalClusteringCoefficientMutateResult emptyFrom(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap
    ) {
        return new LocalClusteringCoefficientMutateResult(
            0,
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractResultBuilder<LocalClusteringCoefficientMutateResult> {

        double averageClusteringCoefficient = 0;

        public Builder withAverageClusteringCoefficient(double averageClusteringCoefficient) {
            this.averageClusteringCoefficient = averageClusteringCoefficient;
            return this;
        }

        @Override
        public LocalClusteringCoefficientMutateResult build() {
            return new LocalClusteringCoefficientMutateResult(
                averageClusteringCoefficient,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
