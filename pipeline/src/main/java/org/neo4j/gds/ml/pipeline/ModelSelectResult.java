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
package org.neo4j.gds.ml.pipeline;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ValueClass
public interface ModelSelectResult {
    TrainerConfig bestParameters();

    Map<Metric, List<ModelStats>> trainStats();

    Map<Metric, List<ModelStats>> validationStats();

    static ModelSelectResult of(
        TrainerConfig bestConfig,
        Map<? extends Metric, List<ModelStats>> trainStats,
        Map<? extends Metric, List<ModelStats>> validationStats
    ) {
        return ImmutableModelSelectResult.of(bestConfig, trainStats, validationStats);
    }

    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "trainStats", MetricStatsToCypher.convertToCypher(trainStats()),
            "validationStats", MetricStatsToCypher.convertToCypher(validationStats())
        );
    }

    final class MetricStatsToCypher {

        private MetricStatsToCypher() {}

        static Map<String,List<Map<String, Object>>> convertToCypher(Map<Metric, List<ModelStats>> metricStats) {
            return metricStats.entrySet().stream().collect(Collectors.toMap(
                entry -> entry.getKey().name(),
                value -> value.getValue().stream().map(ModelStats::toMap).collect(Collectors.toList())
            ));
        }

    }

}
