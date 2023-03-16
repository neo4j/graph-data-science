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
package org.neo4j.gds.betweenness;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;

@SuppressWarnings("unused")
public class StatsResult extends StandardStatsResult {

    public final Map<String, Object> centralityDistribution;

    StatsResult(
        @Nullable Map<String, Object> centralityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.centralityDistribution = centralityDistribution;
    }

    static final class Builder extends AbstractCentralityResultBuilder<StatsResult> {
        protected Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public StatsResult buildResult() {
            return new StatsResult(
                centralityHistogram,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                config.toMap()
            );
        }
    }
}
