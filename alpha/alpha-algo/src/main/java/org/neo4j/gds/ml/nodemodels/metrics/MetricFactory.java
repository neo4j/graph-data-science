/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.metrics;

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class MetricFactory {

    private MetricFactory() {}

    public enum MetricType {
        F1_WEIGHTED,
        F1_MACRO,
        ACCURACY
    }

    public static Metric create(MetricType metricType, HugeLongArray targets) {
        switch(metricType) {
            case F1_WEIGHTED:
                return new F1Weighted(targets);
            case F1_MACRO:
                return new F1Macro(targets);
            case ACCURACY:
                return new AccuracyMetric();
            default:
                throw new IllegalArgumentException(formatWithLocale("Unsupported metric type: %s", metricType.name()));
        }
    }

}
