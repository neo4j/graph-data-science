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

public final class WccWriteResult extends WccStatsResult {

    public final long writeMillis;
    public final long nodePropertiesWritten;

    public WccWriteResult(
        long componentCount,
        Map<String, Object> componentDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long writeMillis,
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
        this.writeMillis = writeMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static WccWriteResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new WccWriteResult(
            0,
            Collections.emptyMap(),
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.mutateOrWriteMillis,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractCommunityResultBuilder<WccWriteResult> {

        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected WccWriteResult buildResult() {
            return new WccWriteResult(
                maybeCommunityCount.orElse(0L),
                communityHistogramOrNull(),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
