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
package org.neo4j.gds.procedures.community.wcc;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.Map;

@SuppressWarnings("unused")
public class WccStatsResult extends StandardStatsResult {

    public final long componentCount;
    public final Map<String, Object> componentDistribution;

    public WccStatsResult(
        long componentCount,
        Map<String, Object> componentDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.componentCount = componentCount;
        this.componentDistribution = componentDistribution;
    }

    public static class Builder extends AbstractCommunityResultBuilder<WccStatsResult> {

        public Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected WccStatsResult buildResult() {
            return new WccStatsResult(
                maybeCommunityCount.orElse(0L),
                communityHistogramOrNull(),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                config.toMap()
            );
        }
    }
}
