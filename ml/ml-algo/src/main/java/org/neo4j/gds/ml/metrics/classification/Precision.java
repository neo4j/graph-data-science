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
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.openjdk.jol.util.Multiset;

import java.util.Comparator;
import java.util.Objects;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Precision implements ClassificationMetric {

    public static final String NAME = "PRECISION";

    private final long positiveTarget;

    public Precision(long positiveTarget) {
        this.positiveTarget = positiveTarget;
    }

    @Override
    public double compute(HugeIntArray targets, HugeIntArray predictions, Multiset<Long> ignore, LocalIdMap localIdMap) {
        assert (targets.size() == predictions.size()) : formatWithLocale(
                    "Metrics require equal length targets and predictions. Sizes are %d and %d respectively.",
                    targets.size(),
                    predictions.size()
                );

        var localPositiveTarget = localIdMap.toMapped(positiveTarget);

        long truePositives = 0L;
        long falsePositives = 0L;
        for (long row = 0; row < targets.size(); row++) {

            long targetClass = targets.get(row);
            long predictedClass = predictions.get(row);

            var predictedIsPositive = predictedClass == localPositiveTarget;
            if (!predictedIsPositive) continue;

            var targetIsPositive = targetClass == localPositiveTarget;

            if (targetIsPositive) {
                truePositives++;
            }
            else {
                falsePositives++;
            }
        }

        var result = truePositives / (truePositives + falsePositives + EPSILON);
        assert result <= 1.0;
        return result;
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

    @Override
    public String name() {
        return formatWithLocale("%s(class=%d)", NAME, positiveTarget);
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }
}
