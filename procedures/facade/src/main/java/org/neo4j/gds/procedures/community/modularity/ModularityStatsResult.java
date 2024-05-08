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
package org.neo4j.gds.procedures.community.modularity;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.Map;

public class ModularityStatsResult extends StandardStatsResult {
    public final long nodeCount;
    public final long relationshipCount;
    public final long communityCount;
    public final double modularity;

    public ModularityStatsResult(
        long nodeCount,
        long relationshipCount,
        long communityCount,
        double modularity,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        this.communityCount = communityCount;
        this.modularity = modularity;
    }

    public static class StatsBuilder extends AbstractCommunityResultBuilder<ModularityStatsResult> {

        double modularity;
        private long relationshipCount;
        private long communityCount;

        public StatsBuilder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        public StatsBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        public StatsBuilder withRelationshipCount(long relationshipCount) {
            this.relationshipCount = relationshipCount;
            return this;
        }

        public StatsBuilder withCommunityCount(long communityCount) {
            this.communityCount = communityCount;
            return this;
        }

        @Override
        protected ModularityStatsResult buildResult() {
            return new ModularityStatsResult(
                nodeCount,
                relationshipCount,
                communityCount,
                modularity,
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                config.toMap()
            );
        }
    }

}
