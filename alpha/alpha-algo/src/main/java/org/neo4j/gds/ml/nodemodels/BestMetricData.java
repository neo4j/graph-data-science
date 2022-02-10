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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Map;

/**
 * The computed metrics for the best model.
 */
@ValueClass
public interface BestMetricData {

    /**
     * Train metric
     * @return the metric stats on the train set
     */
    BestModelStats train();

    /**
     * Validation metric
     * @return the metric stats on the validation set
     */
    BestModelStats validation();

    /**
     * Outer train metric
     * @return the metric stats on the outer training set
     */
    double outerTrain();

    /**
     * Test metric
     * @return the metric value on the test set (holdout)
     */
    double test();

    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "outerTrain", outerTrain(),
            "test", test(),
            "validation", validation().toMap(),
            "train", train().toMap()
        );
    }

    static BestMetricData of(BestModelStats train, BestModelStats validation, double outerTrain, double test) {
        return ImmutableBestMetricData.of(train, validation, outerTrain, test);
    }

    static long estimateMemory() {
        return MemoryUsage.sizeOfInstance(ImmutableBestModelStats.class) * 2 + Double.BYTES * 2;
    }
}

