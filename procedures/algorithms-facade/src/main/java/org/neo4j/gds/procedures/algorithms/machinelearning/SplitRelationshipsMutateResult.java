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

public class SplitRelationshipsMutateResult {
    public final long preProcessingMillis;
    public final long computeMillis;
    public final long mutateMillis;
    public final long relationshipsWritten;
    public final Map<String, Object> configuration;

    public SplitRelationshipsMutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long relationshipsWritten,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.mutateMillis = mutateMillis;
        this.relationshipsWritten = relationshipsWritten;
        this.configuration = configuration;
    }

    public static SplitRelationshipsMutateResult emptyFrom(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap
    ) {
        return new SplitRelationshipsMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractResultBuilder<SplitRelationshipsMutateResult> {
        @Override
        public SplitRelationshipsMutateResult build() {
            return new SplitRelationshipsMutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                relationshipsWritten,
                config.toMap()
            );
        }
    }
}
