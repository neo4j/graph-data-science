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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.ml.nodemodels.metrics.Metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.gds.ml.nodemodels.ModelStats.COMPARE_AVERAGE;

final class StatsMap {

    private final Map<Metric, List<ModelStats>> map;

    static StatsMap create(List<Metric> metrics) {
        Map<Metric, List<ModelStats>> map = new HashMap<>();
        metrics.forEach(metric -> map.put(metric, new ArrayList<>()));
        return new StatsMap(map);
    }

    private StatsMap(Map<Metric, List<ModelStats>> map) {
        this.map = map;
    }

    void add(Metric metric, ModelStats modelStats) {
        map.get(metric).add(modelStats);
    }

    ModelStats pickBestModelStats(Metric metric) {
        var modelStats = map.get(metric);
        return Collections.max(modelStats, COMPARE_AVERAGE);
    }

    Map<Metric, List<ModelStats>> getRawMap() {
        return map;
    }
}
