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
package org.neo4j.gds.procedures.algorithms.embeddings;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class FastRPStatsResult {
    public final long nodeCount;
    public final long preProcessingMillis;
    public final long computeMillis;
    public final Map<String, Object> configuration;

    public FastRPStatsResult(
        long nodeCount,
        long preProcessingMillis,
        long computeMillis,
        Map<String, Object> config
    ) {
        this.nodeCount = nodeCount;
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.configuration = config;
    }

    static FastRPStatsResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new FastRPStatsResult(0, timings.preProcessingMillis, timings.computeMillis, configurationMap);
    }

    public static final class Builder extends AbstractResultBuilder<FastRPStatsResult> {
        @Override
        public FastRPStatsResult build() {
            return new FastRPStatsResult(
                nodeCount,
                preProcessingMillis,
                computeMillis,
                config.toMap()
            );
        }
    }
}
