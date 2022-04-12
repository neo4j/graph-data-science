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
package org.neo4j.gds.ml.metrics.regression;

import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.ml.metrics.Metric;

public enum RegressionMetrics implements Metric {
    MEAN_SQUARED_ERROR {
        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            long numberOfExamples = targets.size();
            assert numberOfExamples == predictions.size();

            double squaredError = 0;
            for (long i = 0; i < numberOfExamples; i++) {
                var error = predictions.get(i) - targets.get(i);
                squaredError += error * error;
            }

            return squaredError / numberOfExamples;
        }
    },
    ROOT_MEAN_SQUARED_ERROR {
        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            return Math.sqrt(MEAN_SQUARED_ERROR.compute(targets, predictions));
        }
    },
    MEAN_ABSOLUTE_ERROR {
        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            long numberOfExamples = targets.size();
            assert numberOfExamples == predictions.size();

            double totalError = 0;
            for (long i = 0; i < numberOfExamples; i++) {
                totalError += Math.abs(targets.get(i) - predictions.get(i));
            }

            return totalError / numberOfExamples;
        }
    };

    public abstract double compute(HugeDoubleArray targets, HugeDoubleArray predictions);
}
