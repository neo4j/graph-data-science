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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class LouvainWriteResult extends LouvainStatsResult {
    public final long writeMillis;
    public final long nodePropertiesWritten;

    public LouvainWriteResult(
        double modularity,
        List<Double> modularities,
        long ranLevels,
        long communityCount,
        Map<String, Object> communityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long writeMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            modularity,
            modularities,
            ranLevels,
            communityCount,
            communityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.writeMillis = writeMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static LouvainWriteResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new LouvainWriteResult(
            0,
            Collections.emptyList(),
            0,
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
}
