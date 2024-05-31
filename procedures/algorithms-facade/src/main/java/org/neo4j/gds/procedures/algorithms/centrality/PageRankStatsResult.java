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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.Map;

public class PageRankStatsResult extends StandardStatsResult {
    public final long ranIterations;
    public final boolean didConverge;
    public final Map<String, Object> centralityDistribution;

    public PageRankStatsResult(
        long ranIterations,
        boolean didConverge,
        Map<String, Object> centralityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.ranIterations = ranIterations;
        this.didConverge = didConverge;
        this.centralityDistribution = centralityDistribution;
    }

    public static class Builder extends PageRankProcCompanion.PageRankResultBuilder<PageRankStatsResult> {
        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public PageRankStatsResult buildResult() {
            return new PageRankStatsResult(
                ranIterations,
                didConverge,
                centralityHistogram,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                config.toMap()
            );
        }
    }
}
