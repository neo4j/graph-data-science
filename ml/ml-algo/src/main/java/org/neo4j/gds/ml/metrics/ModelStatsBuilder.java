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
package org.neo4j.gds.ml.metrics;

import org.neo4j.gds.mem.MemoryUsage;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public class ModelStatsBuilder {
    private final Map<Metric, Double> min;
    private final Map<Metric, Double> max;
    private final Map<Metric, Double> sum;
    private final int numberOfSplits;

    public ModelStatsBuilder(int numberOfSplits) {
        this.numberOfSplits = numberOfSplits;
        this.min = new HashMap<>();
        this.max = new HashMap<>();
        this.sum = new HashMap<>();
    }

    public void update(Metric metric, double value) {
        min.merge(metric, value, Math::min);
        max.merge(metric, value, Math::max);
        sum.merge(metric, value, Double::sum);
    }

    public EvaluationScores build(Metric metric) {
        return EvaluationScores.of(
            sum.get(metric) / numberOfSplits,
            min.get(metric),
            max.get(metric)
        );
    }

    public Map<Metric, EvaluationScores> build() {
        return sum.keySet().stream()
            .collect(
            Collectors.toMap(Function.identity(), this::build)
        );
    }

    public static long sizeInBytes(long numberOfMetrics) {
        int numberOfStats = 3;
        long statsMapEntries = numberOfStats * (sizeOfInstance(HashMap.class) + numberOfMetrics * Double.BYTES);
        return MemoryUsage.sizeOfInstance(ModelStatsBuilder.class) + statsMapEntries;
    }
}
