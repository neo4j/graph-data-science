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
package org.neo4j.gds.ml.metrics.classification;

import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.openjdk.jol.util.Multiset;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GlobalAccuracy implements ClassificationMetric{

    //Class name clashes with Accuracy(local).
    //But user input does not define classes for GlobalAccuracy.
    public static final String NAME = "ACCURACY";

    public GlobalAccuracy(LocalIdMap classIdMap) {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }

    @Override
    public double compute(HugeIntArray targets, HugeIntArray predictions, Multiset<Long> globalClassCounts) {
        long accuratePredictions = 0;
        assert targets.size() == predictions.size() : formatWithLocale(
            "Metrics require equal length targets and predictions. Sizes are %d and %d respectively.",
            targets.size(),
            predictions.size());

        for (long row = 0; row < targets.size(); row++) {
            long targetClass = targets.get(row);
            long predictedClass = predictions.get(row);
            if (predictedClass == targetClass) {
                accuratePredictions++;
            }
        }

        if (targets.size() == 0) {
            return 0.0;
        }
        return BigDecimal.valueOf(accuratePredictions)
            .divide(BigDecimal.valueOf(targets.size()), 8, RoundingMode.UP)
            .doubleValue();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GlobalAccuracy;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
