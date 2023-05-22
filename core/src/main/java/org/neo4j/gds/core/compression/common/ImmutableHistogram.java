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
package org.neo4j.gds.core.compression.common;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;

import java.util.Map;

public class ImmutableHistogram {

    public static final ImmutableHistogram EMPTY = new ImmutableHistogram(new Histogram(0));

    private final AbstractHistogram histogram;

    public ImmutableHistogram(AbstractHistogram histogram) {
        this.histogram = histogram;
    }

    public long minValue() {
        return histogram.getMinValue();
    }

    public double mean() {
        return histogram.getMean();
    }

    public long maxValue() {
        return histogram.getMaxValue();
    }

    public long valueAtPercentile(double percentile) {
        return histogram.getValueAtPercentile(percentile);
    }

    public Map<String, Object> toMap() {
        return Map.of(
            "min", minValue(),
            "mean", mean(),
            "max", maxValue(),
            "p50", valueAtPercentile(50),
            "p75", valueAtPercentile(75),
            "p90", valueAtPercentile(90),
            "p95", valueAtPercentile(95),
            "p99", valueAtPercentile(99),
            "p999", valueAtPercentile(99.9)
        );
    }
}
