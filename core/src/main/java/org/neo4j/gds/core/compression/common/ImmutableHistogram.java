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
import org.neo4j.gds.core.compression.BoundedHistogram;

import java.util.Map;

public interface ImmutableHistogram {

    ImmutableHistogram EMPTY = new Empty();

    long minValue();

    double mean();

    long maxValue();

    long valueAtPercentile(double percentile);

    default Map<String, Object> toMap() {
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

    static ImmutableHistogram of(AbstractHistogram abstractHistogram) {
        return new ImmutableHistogram() {
            @Override
            public long minValue() {
                return abstractHistogram.getMinValue();
            }

            @Override
            public double mean() {
                return abstractHistogram.getMean();
            }

            @Override
            public long maxValue() {
                return abstractHistogram.getMaxValue();
            }

            @Override
            public long valueAtPercentile(double percentile) {
                return abstractHistogram.getValueAtPercentile(percentile);
            }
        };
    }

    static ImmutableHistogram of(BoundedHistogram boundedHistogram) {
        return new ImmutableHistogram() {
            @Override
            public long minValue() {
                return boundedHistogram.min();
            }

            @Override
            public double mean() {
                return boundedHistogram.mean();
            }

            @Override
            public long maxValue() {
                return boundedHistogram.max();
            }

            @Override
            public long valueAtPercentile(double percentile) {
                return boundedHistogram.percentile((float) percentile);
            }
        };
    }

    class Empty implements ImmutableHistogram {

        @Override
        public long minValue() {
            return 0;
        }

        @Override
        public double mean() {
            return 0;
        }

        @Override
        public long maxValue() {
            return 0;
        }

        @Override
        public long valueAtPercentile(double percentile) {
            return 0;
        }
    }
}
