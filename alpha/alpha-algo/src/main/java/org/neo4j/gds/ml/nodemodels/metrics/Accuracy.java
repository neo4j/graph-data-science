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

public class Accuracy implements Metric {

    // TODO: Accuracy per class clashes with the global Accuracy metric.
    // Let's solve that later, and call this APC for now.
    public static final String NAME = "ACCURACY";

    private final long positiveTarget;

    public Accuracy(long positiveTarget) {
        this.positiveTarget = positiveTarget;
    }

    @Override
    public double compute(
        HugeLongArray targets, HugeLongArray predictions, Multiset<Long> ignore
    ) {
        assert (targets.size() == predictions.size()) : formatWithLocale(
            "Metrics require equal length targets and predictions. Sizes are %d and %d respectively.",
            targets.size(),
            predictions.size()
        );

        if (targets.size() == 0) {
            return 0;
        }

        long accurates = 0L;
        for (long row = 0; row < targets.size(); row++) {

            long targetClass = targets.get(row);
            long predictedClass = predictions.get(row);

            var predictedIsPositive = predictedClass == positiveTarget;
            var targetIsPositive = targetClass == positiveTarget;

            if (predictedIsPositive == targetIsPositive) {
                accurates++;
            }

        }

        var result = ((double) accurates) / targets.size();
        return result;
    }

    public double compute(HugeLongArray targets, HugeLongArray predictions) {
        return compute(targets, predictions, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Accuracy accuracyScore = (Accuracy) o;
        return positiveTarget == accuracyScore.positiveTarget;
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
