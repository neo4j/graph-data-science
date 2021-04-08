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
package org.neo4j.gds.ml.nodemodels.metrics;

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import java.util.Objects;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Precision implements Metric {

    public static final String NAME = "PRECISION";
    private static final double EPSILON = 1e-8;

    private final long positiveTarget;

    public Precision(long positiveTarget) {
        this.positiveTarget = positiveTarget;
    }

    @Override
    public double compute(HugeLongArray targets, HugeLongArray predictions, Multiset<Long> ignore) {
        assert (targets.size() == predictions.size()) : formatWithLocale(
                    "Metrics require equal length targets and predictions. Sizes are %d and %d respectively.",
                    targets.size(),
                    predictions.size()
                );

        long truePositives = 0L;
        long falsePositives = 0L;
        for (long row = 0; row < targets.size(); row++) {

            long targetClass = targets.get(row);
            long predictedClass = predictions.get(row);

            var predictedIsPositive = predictedClass == positiveTarget;
            var targetIsPositive = targetClass == positiveTarget;
            var targetIsNegative = !targetIsPositive;

            if (predictedIsPositive && targetIsPositive) {
                truePositives++;
            }

            if (predictedIsPositive && targetIsNegative) {
                falsePositives++;
            }
        }

        var result = truePositives / (truePositives + falsePositives + EPSILON);
        assert result <= 1.0;
        return result;
    }

    public double compute(HugeLongArray targets, HugeLongArray predictions) {
        return compute(targets, predictions, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Precision precisionScore = (Precision) o;
        return positiveTarget == precisionScore.positiveTarget;
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }

    @Override
    public String toString() {
        return formatWithLocale("%s_class_%d", NAME, positiveTarget);
    }
}
