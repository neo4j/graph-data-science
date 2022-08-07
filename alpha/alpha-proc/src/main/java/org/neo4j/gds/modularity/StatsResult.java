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
package org.neo4j.gds.modularity;

import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;

public class StatsResult extends StandardStatsResult {
    public final long nodeCount;
    public final long relationshipCount;
    public final long communityCount;
    public final double modularity;

    StatsResult(
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

    static class StatsBuilder extends AbstractCommunityResultBuilder<StatsResult> {

        double modularity;
        private long relationshipCount;
        private long communityCount;

        StatsBuilder(ProcedureCallContext context, int concurrency) {
            super(context, concurrency);
        }

        StatsBuilder withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }

        StatsBuilder withRelationshipCount(long relationshipCount) {
            this.relationshipCount = relationshipCount;
            return this;
        }

        StatsBuilder withCommunityCount(long communityCount) {
            this.communityCount = communityCount;
            return this;
        }

        @Override
        protected StatsResult buildResult() {
            return new StatsResult(
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
