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
package org.neo4j.gds.procedures.community.leiden;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

import java.util.List;
import java.util.Map;

public final class LeidenMutateResult extends StatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    public LeidenMutateResult(
        long ranLevels,
        boolean didConverge,
        long nodeCount,
        long communityCount,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        @Nullable Map<String, Object> communityDistribution,
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
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public static class Builder extends AbstractCommunityResultBuilder<LeidenMutateResult> {

        long levels = -1;
        boolean didConverge = false;

        double modularity;
        List<Double> modularities;

        public Builder(ProcedureReturnColumns returnColumns, int concurrency) {
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
        protected LeidenMutateResult buildResult() {
            return new LeidenMutateResult(
                levels,
                didConverge,
                nodeCount,
                maybeCommunityCount.orElse(0L),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                mutateMillis,
                nodePropertiesWritten,
                communityHistogramOrNull(),
                modularities,
                modularity,
                config.toMap()
            );
        }
    }

}
