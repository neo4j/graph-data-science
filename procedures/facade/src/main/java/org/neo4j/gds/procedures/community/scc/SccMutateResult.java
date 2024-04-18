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
package org.neo4j.gds.procedures.community.scc;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardMutateResult;

import java.util.Map;

public class SccMutateResult extends StandardMutateResult {

    public final long componentCount;
    public final Map<String, Object> componentDistribution;
    public final long nodePropertiesWritten;

    public SccMutateResult(
        long componentCount,
        Map<String, Object> componentDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis,mutateMillis, configuration);

        this.componentCount = componentCount;
        this.componentDistribution = componentDistribution;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public static class Builder extends AbstractCommunityResultBuilder<SccMutateResult> {

        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public SccMutateResult buildResult() {
            return new SccMutateResult(
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

        public SccMutateResult.Builder buildHistogram(boolean buildHistogram) {
            this.buildHistogram = buildHistogram;
            return this;
        }

        public SccMutateResult.Builder buildCommunityCount(boolean buildCommunityCount) {
            this.buildCommunityCount = buildCommunityCount;
            return this;
        }

    }
}
