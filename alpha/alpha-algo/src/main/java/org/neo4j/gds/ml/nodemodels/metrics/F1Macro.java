/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class F1Macro implements Metric {

    private final List<F1Score> metrics;

    F1Macro(HugeLongArray targets) {
        Set<Long> distinctTargets = new HashSet<>();
        for (long offset = 0; offset < targets.size(); offset++) {
            distinctTargets.add(targets.get(offset));
        }

        this.metrics = distinctTargets.stream()
            .map(F1Score::new)
            .collect(Collectors.toList());
    }

    @Override
    public double compute(
        HugeLongArray targets, HugeLongArray predictions
    ) {
        return this.metrics
            .stream()
            .mapToDouble(metric -> metric.compute(targets, predictions))
            .average()
            .orElse(-1);
    }
}
