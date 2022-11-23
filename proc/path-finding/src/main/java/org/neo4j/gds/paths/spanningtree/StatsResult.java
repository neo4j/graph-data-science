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
package org.neo4j.gds.paths.spanningtree;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardModeResult;

import java.util.Map;

public class StatsResult extends StandardModeResult {

    public final long effectiveNodeCount;
    public final double totalWeight;

    public StatsResult(
        long preProcessingMillis,
        long computeMillis,
        long effectiveNodeCount,
        double totalWeight,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, configuration);
        this.effectiveNodeCount = effectiveNodeCount;
        this.totalWeight = totalWeight;
    }

    static final class Builder extends AbstractResultBuilder<StatsResult> {

        private long effectiveNodeCount;
        private double totalWeight;

        @Override
        public StatsResult build() {
            return new StatsResult(
                preProcessingMillis,
                computeMillis,
                effectiveNodeCount,
                totalWeight,
                config.toMap()
            );
        }

        Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }

        Builder withTotalWeight(double totalWeight) {
            this.totalWeight = totalWeight;
            return this;
        }

    }
}
