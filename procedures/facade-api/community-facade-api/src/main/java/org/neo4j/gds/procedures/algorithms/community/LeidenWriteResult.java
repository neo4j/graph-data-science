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
import java.util.List;
import java.util.Map;

public final class LeidenWriteResult extends LeidenStatsResult {
    public final long writeMillis;
    public final long nodePropertiesWritten;

    public LeidenWriteResult(
        long ranLevels,
        boolean didConverge,
        long nodeCount,
        long communityCount,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long writeMillis,
        long nodePropertiesWritten,
        Map<String, Object> communityDistribution,
        List<Double> modularities,
        double modularity,
        Map<String, Object> configuration
    ) {
        super(
            ranLevels,
            didConverge,
            nodeCount,
            communityCount,
            communityDistribution,
            modularity,
            modularities,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.writeMillis = writeMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static LeidenWriteResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new LeidenWriteResult(
            0,
            false,
            0,
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.sideEffectMillis,
            0,
            Collections.emptyMap(),
            Collections.emptyList(),
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractCommunityResultBuilder<LeidenWriteResult> {
        long levels = -1;
        boolean didConverge = false;

        double modularity;
        List<Double> modularities;

        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        public Builder withLevels(long levels) {
            this.levels = levels;
            return this;
        }

        public Builder withDidConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        public Builder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        public Builder withModularities(List<Double> modularities) {
            this.modularities = modularities;
            return this;
        }

        @Override
        protected LeidenWriteResult buildResult() {
            return new LeidenWriteResult(
                levels,
                didConverge,
                nodeCount,
                maybeCommunityCount.orElse(0L),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodePropertiesWritten,
                communityHistogramOrNull(),
                modularities,
                modularity,
                config.toMap()
            );
        }
    }
}
