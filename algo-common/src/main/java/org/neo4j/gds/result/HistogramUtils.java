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

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.DoubleHistogram;

import java.util.Map;

import static java.util.Map.entry;

public final class HistogramUtils {

    private HistogramUtils() {}

    public static Map<String, Object> similaritySummary(DoubleHistogram histogram) {
        return Map.ofEntries(
            entry("min", histogram.getMinValue()),
            entry("max", histogram.getMaxValue()),
            entry("mean", histogram.getMean()),
            entry("stdDev", histogram.getStdDeviation()),
            entry("p1", histogram.getValueAtPercentile(1)),
            entry("p5", histogram.getValueAtPercentile(5)),
            entry("p10", histogram.getValueAtPercentile(10)),
            entry("p25", histogram.getValueAtPercentile(25)),
            entry("p50", histogram.getValueAtPercentile(50)),
            entry("p75", histogram.getValueAtPercentile(75)),
            entry("p90", histogram.getValueAtPercentile(90)),
            entry("p95", histogram.getValueAtPercentile(95)),
            entry("p99", histogram.getValueAtPercentile(99)),
            entry("p100", histogram.getValueAtPercentile(100))
        );
    }

    public static Map<String, Object> communitySummary(AbstractHistogram histogram) {
        return Map.ofEntries(
            entry("min", histogram.getMinValue()),
            entry("max", histogram.getMaxValue()),
            entry("mean", histogram.getMean()),
            entry("p1", histogram.getValueAtPercentile(1)),
            entry("p5", histogram.getValueAtPercentile(5)),
            entry("p10", histogram.getValueAtPercentile(10)),
            entry("p25", histogram.getValueAtPercentile(25)),
            entry("p50", histogram.getValueAtPercentile(50)),
            entry("p75", histogram.getValueAtPercentile(75)),
            entry("p90", histogram.getValueAtPercentile(90)),
            entry("p95", histogram.getValueAtPercentile(95)),
            entry("p99", histogram.getValueAtPercentile(99)),
            entry("p999", histogram.getValueAtPercentile(99.9))

        );
    }

    static Map<String, Object> centralitySummary(DoubleHistogram histogram) {
        return Map.of(
            "min", histogram.getMinValue(),
            "mean", histogram.getMean(),
            "max", histogram.getMaxValue(),
            "p50", histogram.getValueAtPercentile(50),
            "p75", histogram.getValueAtPercentile(75),
            "p90", histogram.getValueAtPercentile(90),
            "p95", histogram.getValueAtPercentile(95),
            "p99", histogram.getValueAtPercentile(99),
            "p999", histogram.getValueAtPercentile(99.9)
        );
    }

}
