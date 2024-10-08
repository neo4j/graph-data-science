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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

import java.util.Collections;
import java.util.Map;

public final class WccMutateResult extends WccStatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    public WccMutateResult(
        long componentCount,
        Map<String, Object> componentDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            componentCount,
            componentDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public static WccMutateResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new WccMutateResult(
            0,
            Collections.emptyMap(),
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.sideEffectMillis,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractCommunityResultBuilder<WccMutateResult> {

        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected WccMutateResult buildResult() {
            return new WccMutateResult(
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
