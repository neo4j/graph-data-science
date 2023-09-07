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
package org.neo4j.gds.procedures.community.labelpropagation;

import org.neo4j.gds.api.ProcedureReturnColumns;

import java.util.Map;

public final class LabelPropagationMutateResult extends LabelPropagationStatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    public LabelPropagationMutateResult(
        long ranIterations,
        boolean didConverge,
        long communityCount,
        Map<String, Object> communityDistribution,
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
            communityCount,
            communityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public static Builder builder(ProcedureReturnColumns returnColumns, int concurrency) {
        return new Builder(returnColumns, concurrency);
    }

    public static class Builder extends LabelPropagationResultBuilder<LabelPropagationMutateResult> {

        Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected LabelPropagationMutateResult buildResult() {
            return new LabelPropagationMutateResult(
                ranIterations,
                didConverge,
                maybeCommunityCount.orElse(0L),
                communityHistogramOrNull(),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
