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
package org.neo4j.gds.procedures.pipelines;

import org.HdrHistogram.ConcurrentDoubleHistogram;
import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.core.ProcedureConstants;
import org.neo4j.gds.result.HistogramUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class HdrBackedGdsHistogram implements GdsHistogram {
    private static final String OUT_OF_RANGE_MESSAGE = "is out of bounds for histogram, current covered range";
    private static final double MIN_RESOLUTION = 1E-6;

    private final DoubleHistogram delegate;
    private final AtomicBoolean failed = new AtomicBoolean(false);

    HdrBackedGdsHistogram() {
        this(new ConcurrentDoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT));
    }

    HdrBackedGdsHistogram(DoubleHistogram delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onPredictedLink(double value) {
        if (failed.get()) {
            return;
        }
        try {
            delegate.recordValue(value < MIN_RESOLUTION ? 0.0 : value);
        } catch (ArrayIndexOutOfBoundsException e) {
            if (e.getMessage() != null && e.getMessage().contains(OUT_OF_RANGE_MESSAGE)) {
                failed.set(true);
                return;
            }
            throw e;
        }
    }

    @Override
    public Map<String, Object> finalise() {
        if (failed.get()) {
            return HistogramUtils.failure();
        }
        return HistogramUtils.similaritySummary(delegate);
    }
}
