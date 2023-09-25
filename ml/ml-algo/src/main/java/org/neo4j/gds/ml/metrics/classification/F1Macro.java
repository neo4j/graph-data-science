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
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;

public class F1Macro implements ClassificationMetric{

    public static final String NAME = "F1_MACRO";

    private final LocalIdMap classIdMap;

    public F1Macro(LocalIdMap classIdMap) {
        this.classIdMap = classIdMap;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }

    @Override
    public double compute(HugeIntArray targets, HugeIntArray predictions) {
        var metrics = classIdMap.getMappings()
            .map(idMap -> new F1Score(idMap.key, idMap.value))
            .collect(Collectors.toList());

        return metrics
            .stream()
            .mapToDouble(metric -> metric.compute(targets, predictions))
            .average()
            .orElse(-1);
    }

    @Override
    public boolean equals(Object o) {
        return o != null && this.getClass().equals(o.getClass());
    }

    @Override
    public int hashCode() {
        return Objects.hash(classIdMap);
    }

    @Override
    public String toString() {
        return NAME;
    }
}
