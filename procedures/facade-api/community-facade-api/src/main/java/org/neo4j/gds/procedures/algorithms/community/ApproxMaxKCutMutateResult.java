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
import org.neo4j.gds.procedures.algorithms.results.StandardMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Map;

public final class ApproxMaxKCutMutateResult extends StandardMutateResult {
    public final long nodePropertiesWritten;
    public final double cutCost;

    public ApproxMaxKCutMutateResult(
        long nodePropertiesWritten,
        double cutCost,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        Map<String, Object> config
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            mutateMillis,
            config
        );
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.cutCost = cutCost;
    }

    public static ApproxMaxKCutMutateResult emptyFrom(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap
    ) {
        return new ApproxMaxKCutMutateResult(
            0,
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.sideEffectMillis,
            configurationMap
        );
    }

    public static final class Builder extends AbstractResultBuilder<ApproxMaxKCutMutateResult> {
        private final double cutCost;

        public Builder(double cutCost) {
            this.cutCost = cutCost;
        }

        @Override
        public ApproxMaxKCutMutateResult build() {
            return new ApproxMaxKCutMutateResult(
                nodePropertiesWritten,
                cutCost,
                preProcessingMillis,
                computeMillis,
                0L,
                mutateMillis,
                config.toMap()
            );
        }
    }
}
