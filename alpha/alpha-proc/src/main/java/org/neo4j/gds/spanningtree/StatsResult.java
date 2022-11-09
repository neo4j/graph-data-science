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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;

public class StatsResult extends StandardStatsResult {

    public final long effectiveNodeCount;

    public StatsResult(
        long preProcessingMillis,
        long computeMillis,
        long effectiveNodeCount,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, 0, configuration);
        this.effectiveNodeCount = effectiveNodeCount;
    }

    static final class Builder extends AbstractResultBuilder<StatsResult> {

        private long effectiveNodeCount;

        @Override
        public StatsResult build() {
            return new StatsResult(
                preProcessingMillis,
                computeMillis,
                effectiveNodeCount,
                config.toMap()
            );
        }

        public Builder withEffectiveNodeCount(long effectiveNodeCount) {
            this.effectiveNodeCount = effectiveNodeCount;
            return this;
        }


    }
}
