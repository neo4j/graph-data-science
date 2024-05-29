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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.config.WritePropertyConfig;

import java.util.Collections;
import java.util.Map;

public final class AlphaHarmonicWriteResult {
    public final long nodes;
    public final String writeProperty;
    public final long writeMillis;
    public final long computeMillis;
    public final long preProcessingMillis;
    public final Map<String, Object> centralityDistribution;

    AlphaHarmonicWriteResult(
        long nodes,
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        String writeProperty,
        Map<String, Object> centralityDistribution
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;

        this.writeProperty = writeProperty;
        this.centralityDistribution = centralityDistribution;
        this.nodes = nodes;
    }

    static AlphaHarmonicWriteResult emptyFrom(
        AlgorithmProcessingTimings timings,
        WritePropertyConfig configuration
    ) {
        return new AlphaHarmonicWriteResult(
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            configuration.writeProperty(),
            Collections.emptyMap()
        );
    }
}
