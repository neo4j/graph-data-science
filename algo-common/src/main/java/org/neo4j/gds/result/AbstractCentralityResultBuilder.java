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
package org.neo4j.gds.result;

import org.HdrHistogram.DoubleHistogram;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.statistics.CentralityStatistics;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Map;
import java.util.Optional;
import java.util.function.LongToDoubleFunction;

public abstract class AbstractCentralityResultBuilder<WRITE_RESULT> extends AbstractResultBuilder<WRITE_RESULT> {

    private final int concurrency;
    protected boolean buildHistogram;

    protected long postProcessingMillis = -1L;
    protected Optional<DoubleHistogram> maybeCentralityHistogram = Optional.empty();

    protected @Nullable Map<String, Object> centralityHistogramOrNull() {
        return maybeCentralityHistogram.map(histogram -> MapUtil.map(
            "min", histogram.getMinValue(),
            "mean", histogram.getMean(),
            "max", histogram.getMaxValue(),
            "p50", histogram.getValueAtPercentile(50),
            "p75", histogram.getValueAtPercentile(75),
            "p90", histogram.getValueAtPercentile(90),
            "p95", histogram.getValueAtPercentile(95),
            "p99", histogram.getValueAtPercentile(99),
            "p999", histogram.getValueAtPercentile(99.9)
        )).orElse(null);
    }

    protected LongToDoubleFunction centralityFunction = null;

    protected AbstractCentralityResultBuilder(
        ProcedureCallContext callContext,
        int concurrency
    ) {
        this.buildHistogram = callContext
            .outputFields()
            .anyMatch(s -> s.equalsIgnoreCase("centralityDistribution"));
        this.concurrency = concurrency;
    }

    protected abstract WRITE_RESULT buildResult();

    public AbstractCentralityResultBuilder<WRITE_RESULT> withCentralityFunction(LongToDoubleFunction centralityFunction) {
        this.centralityFunction = centralityFunction;
        return this;
    }

    @Override
    public WRITE_RESULT build() {
        final ProgressTimer timer = ProgressTimer.start();

        if (buildHistogram && centralityFunction != null) {
            maybeCentralityHistogram = Optional.of(CentralityStatistics.histogram(nodeCount, centralityFunction,Pools.DEFAULT, concurrency));
        }
        timer.stop();
        this.postProcessingMillis = timer.getDuration();

        return buildResult();
    }

}
