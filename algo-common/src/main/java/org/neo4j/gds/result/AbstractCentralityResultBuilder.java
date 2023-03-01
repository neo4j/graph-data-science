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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.scaling.LogScaler;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongToDoubleFunction;

import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public abstract class AbstractCentralityResultBuilder<WRITE_RESULT> extends AbstractResultBuilder<WRITE_RESULT> {

    static final String HISTOGRAM_ERROR_KEY = "Error";

    private final int concurrency;
    private final boolean buildHistogram;
    private final Map<String, Object> histogramError;

    private LongToDoubleFunction centralityFunction;
    private ScalerFactory scaler;

    protected long postProcessingMillis = -1L;
    protected Map<String, Object> centralityHistogram;

    protected AbstractCentralityResultBuilder(
        ProcedureReturnColumns returnColumns,
        int concurrency
    ) {
        this.buildHistogram = returnColumns.contains("centralityDistribution");
        this.concurrency = concurrency;
        this.histogramError = new HashMap<>();
    }

    protected abstract WRITE_RESULT buildResult();

    public AbstractCentralityResultBuilder<WRITE_RESULT> withCentralityFunction(LongToDoubleFunction centralityFunction) {
        this.centralityFunction = centralityFunction;
        return this;
    }

    public AbstractCentralityResultBuilder<WRITE_RESULT> withScalerVariant(ScalerFactory scaler) {
        this.scaler = scaler;
        return this;
    }

    @Override
    public WRITE_RESULT build() {
        var timer = ProgressTimer.start();
        var maybeCentralityHistogram = computeCentralityHistogram();
        this.centralityHistogram = centralityHistogramResult(maybeCentralityHistogram);

        timer.stop();
        this.postProcessingMillis = timer.getDuration();

        return buildResult();
    }

    @NotNull
    private Optional<DoubleHistogram> computeCentralityHistogram() {
        var logScaler = scaler != null && scaler.type().equals(LogScaler.TYPE);
        if (buildHistogram && centralityFunction != null) {
            if (logScaler) {
                this.histogramError.put(
                    HISTOGRAM_ERROR_KEY,
                    "Unable to create histogram when using scaler of type " + toUpperCaseWithLocale(LogScaler.TYPE)
                );
            } else {
                try {
                    return Optional.of(CentralityStatistics.histogram(
                        nodeCount,
                        centralityFunction,
                        Pools.DEFAULT,
                        concurrency
                    ));
                } catch (ArrayIndexOutOfBoundsException e) {
                    // waiting for: https://github.com/HdrHistogram/HdrHistogram/issues/190 to be resolved
                    if (e.getMessage().contains("is out of bounds for histogram, current covered range")) {
                        this.histogramError.put(
                            HISTOGRAM_ERROR_KEY,
                            "Unable to create histogram due to range of scores exceeding implementation limits."
                        );
                    } else {
                        throw e;
                    }
                }
            }
        }
        return Optional.empty();
    }

    private Map<String, Object> centralityHistogramResult(Optional<DoubleHistogram> maybeHistogram) {
        return maybeHistogram.map(HistogramUtils::centralitySummary).orElse(histogramError);
    }
}
