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

import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.mem.MemoryUsage.sizeOfInstance;

public final class StatsMap {

    public static MemoryEstimation memoryEstimation(int numberOfMetricsSpecifications, int numberOfModelCandidates) {
        int fudgedNumberOfClasses = 1000;
        return memoryEstimation(numberOfMetricsSpecifications, numberOfModelCandidates, fudgedNumberOfClasses);
    }

    public static MemoryEstimation memoryEstimation(int numberOfMetricsSpecifications, int numberOfModelCandidates, int numberOfClasses) {
        var numberOfMetrics = numberOfMetricsSpecifications * numberOfClasses;
        var numberOfModelStats = numberOfMetrics * numberOfModelCandidates;
        var sizeOfOneModelStatsInBytes = sizeOfInstance(ImmutableModelStats.class);
        var sizeOfAllModelStatsInBytes = sizeOfOneModelStatsInBytes * numberOfModelStats;
        return MemoryEstimations.builder(StatsMap.class)
            .fixed("array list", sizeOfInstance(ArrayList.class))
            .fixed("model stats", sizeOfAllModelStatsInBytes)
            .build();
    }

    private final Map<Metric, List<ModelStats>> map;

    public static StatsMap create(List<Metric> metrics) {
        Map<Metric, List<ModelStats>> map = new HashMap<>();
        metrics.forEach(metric -> map.put(metric, new ArrayList<>()));
        return new StatsMap(map);
    }

    private StatsMap(Map<Metric, List<ModelStats>> map) {
        this.map = map;
    }

    public void add(Metric metric, ModelStats modelStats) {
        map.get(metric).add(modelStats);
    }

    public ModelStats pickBestModelStats(Metric metric) {
        var modelStats = map.get(metric);
        return Collections.max(modelStats, ModelStats.COMPARE_AVERAGE);
    }

    public Map<Metric, List<ModelStats>> getMap() {
        return map;
    }
}
