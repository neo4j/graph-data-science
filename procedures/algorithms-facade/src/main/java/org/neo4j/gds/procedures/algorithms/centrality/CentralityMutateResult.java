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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;

import java.util.Collections;
import java.util.Map;

public final class CentralityMutateResult extends CentralityStatsResult {
    public final long nodePropertiesWritten;
    public final long mutateMillis;

    public CentralityMutateResult(
        long nodePropertiesWritten,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        Map<String, Object> centralityDistribution,

        Map<String, Object> config
    ) {
        super(
            centralityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            config
        );
        this.nodePropertiesWritten = nodePropertiesWritten;
        this.mutateMillis = mutateMillis;
    }

    public static final class Builder extends AbstractCentralityResultBuilder<CentralityMutateResult> {
        public Builder(ProcedureReturnColumns returnColumns, Concurrency concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        public CentralityMutateResult buildResult() {
            return new CentralityMutateResult(
                nodePropertiesWritten,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                mutateMillis,
                centralityHistogram,
                config.toMap()
            );
        }
    }

    public static CentralityMutateResult emptyFrom(
        AlgorithmProcessingTimings timings,
        Map<String, Object> configurationMap
    ) {
        return new CentralityMutateResult(
            0,
            timings.preProcessingMillis,
            timings.computeMillis,
            0,
            timings.preProcessingMillis,
            Collections.emptyMap(),
            configurationMap
        );
    }
}
