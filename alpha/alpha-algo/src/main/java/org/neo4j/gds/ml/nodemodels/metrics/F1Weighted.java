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

import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.openjdk.jol.util.Multiset;

public class F1Weighted implements Metric {

    private final Multiset<Long> targetCounts;

    F1Weighted(HugeAtomicLongArray targets) {
        this.targetCounts = new Multiset<>();
        for (long offset = 0; offset < targets.size(); offset++) {
            targetCounts.add(targets.get(offset));
        }
    }

    @Override
    public double compute(
        HugeAtomicLongArray targets, HugeAtomicLongArray predictions
    ) {
        return this.targetCounts.keys()
            .stream()
            .mapToDouble(target -> {
                var metric = new F1Score(target);
                return targetCounts.count(target) * metric.compute(targets, predictions);
            })
            .sum() / targets.size();
    }
}
