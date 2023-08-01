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

import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Comparator;
import java.util.Objects;

public class F1Weighted implements ClassificationMetric {

    public static final String NAME = "F1_WEIGHTED";

    private final LocalIdMap classIdMap;
    private final LongMultiSet globalClassCounts;

    public F1Weighted(LocalIdMap classIdMap, LongMultiSet globalClassCounts) {
        this.classIdMap = classIdMap;
        this.globalClassCounts = globalClassCounts;
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
        if (globalClassCounts.size() == 0) {
            return 0.0;
        }

        var weightedScores = classIdMap.getMappings().mapToDouble(idMap -> {
            var weight = globalClassCounts.count(idMap.key);
            return weight * new F1Score(idMap.key, idMap.value).compute(targets, predictions);
        });
        return weightedScores.sum() / globalClassCounts.sum();
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
