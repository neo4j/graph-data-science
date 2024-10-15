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

class HdrBackedGdsHistogram implements GdsHistogram {
    private final DoubleHistogram delegate = new ConcurrentDoubleHistogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);

    @Override
    public void onPredictedLink(double value) {
        //HISTOGRAM_PRECISION_DEFAULT hence numberOfSignificantValueDigits is 1E-5, so it can't separate 0 and 1E-5
        //Therefore we can floor at 1E-6 and smaller probabilities between 0 and 1E-6 is unnecessary.
        delegate.recordValue(Math.max(value, 1E-6));
    }

    @Override
    public Map<String, Object> finalise() {
        return HistogramUtils.similaritySummary(delegate);
    }
}
