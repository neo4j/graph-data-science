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
package org.neo4j.gds.ml.models;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface ClassAwareTrainerConfig extends TrainerConfig {
    @Configuration.DoubleRange(min = 0.0)
    default double focusWeight() {
        return 0;
    }

    @Value.Default
    default List<Double> classWeights() {
        return List.of();
    };

    @Configuration.Ignore
    default double[] initializeClassWeights(int numberOfClasses) {
        double[] initializedClassWeights;
        if (classWeights().isEmpty()) {
            initializedClassWeights = new double[numberOfClasses];
            for (int i = 0; i < numberOfClasses; i++) {
                initializedClassWeights[i] = 1;
            }
        } else {
            if (classWeights().size() != numberOfClasses) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The classWeights list %s has %s entries, but it should have %s entries instead, which is the number of classes.",
                    classWeights(),classWeights().size(), numberOfClasses
                ));
            }
            initializedClassWeights = classWeights().stream().mapToDouble(i -> i).toArray();
        }
        return initializedClassWeights;
    }

    @Value.Check
    default void validate() {
        double epsilon = 1e-02;
        if (!classWeights().isEmpty()) {
            var classWeightsSum = classWeights().stream().mapToDouble(Double::doubleValue).sum();
            if (Math.abs(classWeightsSum - 1.0) > epsilon) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The classWeights %s sum up to %s, they should sum up to 1 instead.",
                    classWeights(), classWeightsSum
                ));
            }
        }
    }

}
