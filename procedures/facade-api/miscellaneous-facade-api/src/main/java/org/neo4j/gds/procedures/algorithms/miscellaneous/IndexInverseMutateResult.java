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
package org.neo4j.gds.procedures.algorithms.miscellaneous;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.procedures.algorithms.results.StandardMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class IndexInverseMutateResult extends StandardMutateResult {
    public final long inputRelationships;

    public IndexInverseMutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long postProcessingMillis,
        long inputRelationships,
        Map<String, Object> configuration
    ) {
        super(preProcessingMillis, computeMillis, postProcessingMillis, mutateMillis, configuration);
        this.inputRelationships = inputRelationships;
    }

    public static IndexInverseMutateResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new IndexInverseMutateResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            0,
            0,
            configurationMap
        );
    }

    public static class Builder extends AbstractResultBuilder<IndexInverseMutateResult> {
        private long inputRelationships;

        public Builder withInputRelationships(long inputRelationships) {
            this.inputRelationships = inputRelationships;
            return this;
        }

        @Override
        public IndexInverseMutateResult build() {
            return new IndexInverseMutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                0,
                inputRelationships,
                config.toMap()
            );
        }
    }
}
