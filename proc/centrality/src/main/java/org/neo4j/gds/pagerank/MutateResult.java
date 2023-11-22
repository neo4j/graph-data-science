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
package org.neo4j.gds.pagerank;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankProcCompanion;
import org.neo4j.gds.procedures.centrality.pagerank.PageRankStatsResult;

import java.util.Map;

@SuppressWarnings("unused")
public final class MutateResult extends PageRankStatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    MutateResult(
        long ranIterations,
        boolean didConverge,
        @Nullable Map<String, Object> centralityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            ranIterations,
            didConverge,
            centralityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }


    static class Builder extends PageRankProcCompanion.PageRankResultBuilder<MutateResult> {

        Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public MutateResult buildResult() {
            return new MutateResult(
                ranIterations,
                didConverge,
                centralityHistogram,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
