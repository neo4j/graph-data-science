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

import org.neo4j.gds.collections.ha.HugeIntArray;

import java.util.Comparator;
import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Accuracy implements ClassificationMetric {

    public static final String NAME = "ACCURACY";

    private final long originalTarget;

    private final int internalTarget;

    public Accuracy(long originalTarget, int internalTarget) {
        this.originalTarget = originalTarget;
        this.internalTarget = internalTarget;
    }

    @Override
    public double compute(HugeIntArray targets, HugeIntArray predictions) {
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

            var predictedIsPositive = predictedClass == internalTarget;
            var targetIsPositive = targetClass == internalTarget;

            if (predictedIsPositive == targetIsPositive) {
                accurates++;
            }

        }

        return ((double) accurates) / targets.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Accuracy accuracy = (Accuracy) o;
        return originalTarget == accuracy.originalTarget;
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }

    @Override
    public String toString() {
        return formatWithLocale("%s_class_%d", NAME, originalTarget);
    }

    @Override
    public String name() {
        return formatWithLocale("%s(class=%d)", NAME, originalTarget);
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }
}
