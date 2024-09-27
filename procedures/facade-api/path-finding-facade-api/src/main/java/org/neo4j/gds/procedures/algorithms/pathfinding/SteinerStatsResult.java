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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardModeResult;

import java.util.Map;

public class SteinerStatsResult extends StandardModeResult {

    public final long effectiveNodeCount;
    public final long effectiveTargetNodesCount;
    public final double totalWeight;

    public SteinerStatsResult(
        long preProcessingMillis,
        long computeMillis,
        long effectiveNodeCount,
        long effectiveTargetNodesCount,
        double totalWeight,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, configuration);
        this.effectiveNodeCount = effectiveNodeCount;
        this.totalWeight = totalWeight;
        this.effectiveTargetNodesCount = effectiveTargetNodesCount;
    }

    public static final class Builder extends AbstractResultBuilder<SteinerStatsResult> {

        private long effectiveNodeCount;
        private double totalWeight;
        private long effectiveTargetNodesCount;

        @Override
        public SteinerStatsResult build() {
            return new SteinerStatsResult(
                preProcessingMillis,
                computeMillis,
                effectiveNodeCount,
                effectiveTargetNodesCount,
                totalWeight,
                config.toMap()
            );
        }

        public Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        public Builder withTotalWeight(double totalWeight) {
            this.totalWeight = totalWeight;
            return this;
        }

        public Builder withEffectiveTargetNodesCount(long effectiveTargetNodesCount) {
            this.effectiveTargetNodesCount = effectiveTargetNodesCount;
            return this;
        }

    }
}
