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
package org.neo4j.gds.scaling;

import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface Scaler {

    double CLOSE_TO_ZERO = 1e-15;

    double scaleProperty(long nodeId);

    int dimension();

    Map<String, List<Double>> statistics();

    class ArrayScaler implements Scaler {

        private final List<ScalarScaler> elementScalers;
        private final ProgressTracker progressTracker;

        ArrayScaler(List<ScalarScaler> elementScalers, ProgressTracker progressTracker) {
            this.elementScalers = elementScalers;
            this.progressTracker = progressTracker;
        }

        public void scaleProperty(long nodeId, double[] result, int offset) {
            for (int i = 0; i < dimension(); i++) {
                result[offset + i] = elementScalers.get(i).scaleProperty(nodeId);
            }
            // -1 because we also count progress for the partition
            progressTracker.logProgress(dimension() - 1);
        }

        @Override
        public double scaleProperty(long nodeId) {
            throw new UnsupportedOperationException("Use the other scaleProperty method");
        }

        @Override
        public int dimension() {
            return elementScalers.size();
        }


        @Override
        public Map<String, List<Double>> statistics() {
            return elementScalers.get(0).statistics().keySet().stream().collect(Collectors.toMap(
                Function.identity(),
                stat -> elementScalers
                    .stream()
                    .map(scaler -> scaler.statistics().get(stat).get(0))
                    .collect(Collectors.toList())
            ));
        }
    }

}
