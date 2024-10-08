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
package org.neo4j.gds.procedures.algorithms.machinelearning;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class KGEWriteResult {
    public final long preProcessingMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long relationshipsWritten;
    public final Map<String, Object> configuration;

    KGEWriteResult(
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long relationshipsWritten,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.relationshipsWritten = relationshipsWritten;
        this.configuration = configuration;
    }

    static KGEWriteResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new KGEWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.sideEffectMillis,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractResultBuilder<KGEWriteResult> {
        @Override
        public KGEWriteResult build() {
            return new KGEWriteResult(
                preProcessingMillis,
                computeMillis,
                writeMillis,
                relationshipsWritten,
                config.toMap()
            );
        }
    }
}
