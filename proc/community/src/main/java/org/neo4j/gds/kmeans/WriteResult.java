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
package org.neo4j.gds.kmeans;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;

public class WriteResult extends StatsResult {

    public final long writeMillis;
    public final long nodePropertiesWritten;

    public WriteResult(
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long writeMillis,
        long nodePropertiesWritten,
        @Nullable Map<String, Object> communityDistribution,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, communityDistribution, null, configuration);
        this.writeMillis = writeMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static class Builder extends AbstractCommunityResultBuilder<WriteResult> {

        Builder(
            ProcedureCallContext context,
            int concurrency
        ) {
            super(context, concurrency);
        }

        @Override
        protected WriteResult buildResult() {
            return new WriteResult(
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                writeMillis,
                nodePropertiesWritten,
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }

}
