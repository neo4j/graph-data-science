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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.HdrHistogram.ConcurrentDoubleHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.core.ProcedureConstants;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.result.HistogramUtils;
import org.neo4j.gds.results.StandardMutateResult;

import java.util.Map;

public final class MutateResult extends StandardMutateResult {

    public final long relationshipsWritten;
    public final Map<String, Object> probabilityDistribution;
    public final Map<String, Object> samplingStats;

    private MutateResult(
        long preProcessingMillis,
        long computeMillis,
        long mutateMillis,
        long relationshipsWritten,
        Map<String, Object> configuration,
        Map<String, Object> probabilityDistribution,
        Map<String, Object> samplingStats
    ) {
        super(
            preProcessingMillis,
            computeMillis,
            0L,
            mutateMillis,
            configuration
        );

        this.relationshipsWritten = relationshipsWritten;
        this.probabilityDistribution = probabilityDistribution;
        this.samplingStats = samplingStats;
    }

    static class Builder extends AbstractResultBuilder<MutateResult> {

        private Map<String, Object> samplingStats = null;

        @Nullable
        private ConcurrentDoubleHistogram histogram = null;

        @Override
        public MutateResult build() {
            return new MutateResult(
                preProcessingMillis,
                computeMillis,
                mutateMillis,
                relationshipsWritten,
                config.toMap(),
                histogram == null ? Map.of() : HistogramUtils.similaritySummary(histogram),
                samplingStats
            );
        }

        Builder withHistogram() {
            if (histogram != null) {
                return this;
            }

            this.histogram = new ConcurrentDoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
            return this;
        }

        void recordHistogramValue(double value) {
            if (histogram == null) {
                return;
            }

            histogram.recordValue(value);
        }

        Builder withSamplingStats(Map<String, Object> samplingStats) {
            this.samplingStats = samplingStats;
            return this;
        }
    }
}
